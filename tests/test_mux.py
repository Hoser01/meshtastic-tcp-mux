import importlib.util
import selectors
import socket
import sys
import threading
import time
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("mux", ROOT / "meshtastic_tcp_mux.py")
assert SPEC and SPEC.loader
mux = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = mux
SPEC.loader.exec_module(mux)


class FrameParserTests(unittest.TestCase):
    def test_partial_and_corrupt_frames_resynchronize(self):
        parser = mux.FrameParser("test")
        buf = bytearray()
        raw = mux.pack_frame(b"hello")
        self.assertEqual(parser.feed(buf, b"junk" + raw[:3]), [])
        frames = parser.feed(buf, raw[3:])
        self.assertEqual([f.payload for f in frames], [b"hello"])

        corrupt = bytes((mux.START1, mux.START2, 0xFF, 0xFF))
        frames = parser.feed(buf, corrupt + mux.pack_frame(b"ok"))
        self.assertEqual([f.payload for f in frames], [b"ok"])


class PolicyTests(unittest.TestCase):
    def frame(self, payload):
        return mux.Frame(payload=payload, raw=mux.pack_frame(payload))

    def test_downstream_disconnect_is_never_forwarded(self):
        # ToRadio.disconnect = field 4, varint true.
        allowed, reason = mux.client_frame_allowed(self.frame(b"\x20\x01"))
        self.assertFalse(allowed)
        self.assertIn("disconnect", reason)

    @unittest.skipIf(mux.mesh_pb2 is None, "Meshtastic protobuf package unavailable")
    def test_admin_packet_filter_uses_decoded_portnum(self):
        msg = mux.mesh_pb2.ToRadio()
        msg.packet.decoded.portnum = mux.portnums_pb2.ADMIN_APP
        admin = mux.admin_pb2.AdminMessage()
        admin.set_config.lora.CopyFrom(admin.set_config.lora.__class__())
        msg.packet.decoded.payload = admin.SerializeToString()
        allowed, reason = mux.client_frame_allowed(self.frame(msg.SerializeToString()))
        self.assertFalse(allowed)
        self.assertIn("set_config", reason)

    def test_filter_fails_closed_without_protobuf(self):
        with mock.patch.object(mux, "mesh_pb2", None):
            allowed, reason = mux.client_frame_allowed(self.frame(b"\x0a\x00"))
        self.assertFalse(allowed)
        self.assertIn("protobuf unavailable", reason)


class BackpressureTests(unittest.TestCase):
    def test_slow_client_is_dropped_at_bounded_queue(self):
        service = mux.MeshtasticTcpMux()
        left, right = socket.socketpair()
        self.addCleanup(right.close)
        left.setblocking(False)
        client = mux.Client(left, ("local", 1), 1)
        service.clients[1] = client
        with mock.patch.object(mux, "CLIENT_SEND_QUEUE_MAX_BYTES", 8):
            self.assertFalse(service._queue_client_data(client, b"123456789", None))
        self.assertNotIn(1, service.clients)

    def test_cache_replay_is_byte_bounded_and_nonblocking(self):
        service = mux.MeshtasticTcpMux()
        left, right = socket.socketpair()
        self.addCleanup(left.close)
        self.addCleanup(right.close)
        left.setblocking(False)
        client = mux.Client(left, ("local", 1), 1)
        service.clients[1] = client
        selector = selectors.DefaultSelector()
        self.addCleanup(selector.close)
        selector.register(left, selectors.EVENT_READ, client)
        now = time.time()
        service.cache.extend(
            mux.Frame(b"1234", mux.pack_frame(b"1234"), now) for _ in range(10)
        )
        with mock.patch.object(mux, "CACHE_REPLAY_MAX_BYTES", 20):
            service._replay_cache(client, selector)
        self.assertLessEqual(len(client.txbuf), 20)
        self.assertGreater(len(client.txbuf), 0)


class UpstreamQueueTests(unittest.TestCase):
    def test_frame_waits_for_reconnect_instead_of_immediate_drop(self):
        service = mux.MeshtasticTcpMux()
        service.outbound.put(mux.OutboundItem(1, ("local", 1), mux.Frame(b"x", mux.pack_frame(b"x"))))
        sent = threading.Event()

        def fake_send(_item):
            sent.set()
            service.stop_event.set()

        service._send_to_upstream = fake_send
        thread = threading.Thread(target=service._outbound_loop)
        thread.start()
        time.sleep(0.05)
        self.assertFalse(sent.is_set())
        service._upstream_connected.set()
        thread.join(1)
        self.assertTrue(sent.is_set())


class ServiceIntegrationTests(unittest.TestCase):
    def test_new_and_closing_client_does_not_reset_shared_upstream(self):
        radio_listener = socket.socket()
        radio_listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        radio_listener.bind(("127.0.0.1", 0))
        radio_listener.listen()
        radio_listener.settimeout(0.2)
        listen_probe = socket.socket()
        listen_probe.bind(("127.0.0.1", 0))
        mux_port = listen_probe.getsockname()[1]
        listen_probe.close()
        received = []
        accepts = []
        radio_stop = threading.Event()

        def radio():
            conn = None
            parser = mux.FrameParser("fake-radio")
            buf = bytearray()
            try:
                while not radio_stop.is_set():
                    if conn is None:
                        try:
                            conn, _addr = radio_listener.accept()
                            conn.settimeout(0.1)
                            accepts.append(1)
                        except socket.timeout:
                            continue
                        except OSError:
                            break
                    try:
                        data = conn.recv(4096)
                        if not data:
                            conn.close()
                            conn = None
                            continue
                        received.extend(f.payload for f in parser.feed(buf, data))
                    except socket.timeout:
                        continue
            finally:
                if conn:
                    conn.close()

        radio_thread = threading.Thread(target=radio)
        radio_thread.start()
        old = (
            mux.REAL_NODE_HOST,
            mux.REAL_NODE_PORT,
            mux.LISTEN_HOST,
            mux.LISTEN_PORT,
            mux.FILTER_CLIENT_ADMIN,
        )
        mux.REAL_NODE_HOST = "127.0.0.1"
        mux.REAL_NODE_PORT = radio_listener.getsockname()[1]
        mux.LISTEN_HOST = "127.0.0.1"
        mux.LISTEN_PORT = mux_port
        mux.FILTER_CLIENT_ADMIN = False
        service = mux.MeshtasticTcpMux()
        service_thread = threading.Thread(target=service.run)
        service_thread.start()
        clients = []
        try:
            deadline = time.time() + 2
            while time.time() < deadline:
                try:
                    clients.append(socket.create_connection(("127.0.0.1", mux_port), timeout=0.1))
                    break
                except OSError:
                    time.sleep(0.02)
            self.assertTrue(clients)
            clients.append(socket.create_connection(("127.0.0.1", mux_port)))
            # heartbeat is a valid ToRadio message; disconnect is session-local.
            clients[0].sendall(mux.pack_frame(b"\x3a\x00"))
            clients[1].sendall(mux.pack_frame(b"\x20\x01"))
            clients[1].close()
            time.sleep(0.3)
            self.assertIn(b"\x3a\x00", received)
            self.assertNotIn(b"\x20\x01", received)
            self.assertEqual(len(accepts), 1)
        finally:
            for client in clients:
                try:
                    client.close()
                except OSError:
                    pass
            service.stop()
            service_thread.join(2)
            radio_stop.set()
            radio_listener.close()
            radio_thread.join(1)
            (
                mux.REAL_NODE_HOST,
                mux.REAL_NODE_PORT,
                mux.LISTEN_HOST,
                mux.LISTEN_PORT,
                mux.FILTER_CLIENT_ADMIN,
            ) = old


if __name__ == "__main__":
    unittest.main()
