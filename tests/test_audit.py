import importlib.util
import json
import queue
import selectors
import socket
import sys
import threading
import time
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
import audit_stream as audit

MUX_SPEC = importlib.util.spec_from_file_location("audit_mux", ROOT / "meshtastic_tcp_mux.py")
assert MUX_SPEC and MUX_SPEC.loader
mux = importlib.util.module_from_spec(MUX_SPEC)
sys.modules[MUX_SPEC.name] = mux
MUX_SPEC.loader.exec_module(mux)


@unittest.skipIf(audit.mesh_pb2 is None, "Meshtastic protobuf package unavailable")
class SafeDecodeTests(unittest.TestCase):
    def packet(self, destination=0xFFFFFFFF):
        message = audit.mesh_pb2.ToRadio()
        packet = message.packet
        setattr(packet, "from", 0xA2E9F268)
        packet.to = destination
        packet.id = 0x12345678
        packet.channel = 2
        packet.hop_limit = 3
        packet.hop_start = 4
        packet.want_ack = True
        return message, packet

    def test_text_metadata_never_contains_text(self):
        message, packet = self.packet(0xA0352614)
        packet.decoded.portnum = audit.portnums_pb2.TEXT_MESSAGE_APP
        packet.decoded.payload = b"SECRET TEXT MUST NOT APPEAR"
        metadata, _parsed = audit.decode_frame("client_to_radio", message.SerializeToString())
        rendered = json.dumps(metadata)
        self.assertNotIn("SECRET", rendered)
        self.assertEqual(metadata["packet_id"], 0x12345678)
        self.assertEqual(metadata["from_id"], "!a2e9f268")
        self.assertEqual(metadata["to_id"], "!a0352614")
        self.assertEqual(metadata["application"], "TEXT_MESSAGE_APP")
        self.assertFalse(metadata["encrypted"])

    def test_encrypted_broadcast_reports_headers_only(self):
        message, packet = self.packet()
        packet.encrypted = b"ciphertext-is-private"
        metadata, _parsed = audit.decode_frame("client_to_radio", message.SerializeToString())
        self.assertTrue(metadata["encrypted"])
        self.assertEqual(metadata["to_id"], "!ffffffff")
        self.assertNotIn("ciphertext", json.dumps(metadata))

    def test_queue_status_is_safe(self):
        message = audit.mesh_pb2.FromRadio()
        message.queueStatus.res = 1
        message.queueStatus.free = 2
        message.queueStatus.maxlen = 16
        message.queueStatus.mesh_packet_id = 99
        metadata, parsed = audit.decode_frame("radio_to_clients", message.SerializeToString())
        self.assertTrue(parsed.HasField("queueStatus"))
        self.assertEqual(metadata["queue"]["mesh_packet_id"], 99)

    def test_routing_packet_exposes_correlation_fields_not_payload(self):
        message = audit.mesh_pb2.FromRadio()
        packet = message.packet
        setattr(packet, "from", 0xA2E9F268)
        packet.to = 0xA0352614
        packet.id = 0x12345678
        packet.decoded.portnum = audit.portnums_pb2.ROUTING_APP
        packet.decoded.request_id = 71
        packet.decoded.reply_id = 72
        packet.decoded.payload = b"routing-private-payload"
        metadata, _parsed = audit.decode_frame("radio_to_clients", message.SerializeToString())
        self.assertEqual(metadata["application"], "ROUTING_APP")
        self.assertEqual(metadata["request_id"], 71)
        self.assertEqual(metadata["reply_id"], 72)
        self.assertNotIn("routing-private", json.dumps(metadata))

    def test_blocked_admin_emits_sanitized_audit_record(self):
        class Capture:
            def __init__(self):
                self.records = []

            def emit(self, record):
                self.records.append(record)

        message, packet = self.packet()
        packet.decoded.portnum = audit.portnums_pb2.ADMIN_APP
        packet.decoded.payload = b"admin-private-payload"
        frame = mux.Frame(message.SerializeToString(), mux.pack_frame(message.SerializeToString()))
        service = mux.MeshtasticTcpMux()
        capture = Capture()
        service.audit = capture
        left, right = socket.socketpair()
        self.addCleanup(left.close)
        self.addCleanup(right.close)
        client = mux.Client(left, ("local", 1), 1)
        service.clients[1] = client
        selector = selectors.DefaultSelector()
        self.addCleanup(selector.close)
        selector.register(left, selectors.EVENT_READ, client)
        service._handle_client_frame(client, selector, frame)
        blocked = [r for r in capture.records if r["event"] == "client_frame_blocked"]
        self.assertEqual(len(blocked), 1)
        self.assertEqual(blocked[0]["disposition"], "blocked")
        self.assertNotIn("admin-private", json.dumps(blocked))

    def test_local_source_sentinel_is_resolved_with_provenance(self):
        service = mux.MeshtasticTcpMux()
        service._local_node_num = 0xA2E9F268
        metadata = {
            "packet_id": 0x12345678,
            "from_node": 0,
            "from_id": "!00000000",
            "correlation_id": "packet:00000000:12345678",
        }
        enriched = service._enrich_outbound_metadata(metadata)
        self.assertEqual(enriched["from_id"], "!a2e9f268")
        self.assertTrue(enriched["source_inferred_from_upstream"])
        self.assertEqual(enriched["correlation_id"], "packet:a2e9f268:12345678")

    def test_malformed_frame_reports_class_only(self):
        metadata, parsed = audit.decode_frame("client_to_radio", b"\xff")
        self.assertIsNone(parsed)
        self.assertEqual(metadata["decode_status"], "malformed")
        self.assertEqual(set(metadata), {"decode_status", "error"})


class AuditServerTests(unittest.TestCase):
    def free_port(self):
        probe = socket.socket()
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
        probe.close()
        return port

    def test_consumer_connects_receives_ndjson_and_writes_are_rejected(self):
        server = audit.AuditServer("127.0.0.1", self.free_port(), queue_size=4)
        server.start()
        self.addCleanup(server.stop)
        client = socket.create_connection((server.host, server.port))
        self.addCleanup(client.close)
        line = client.makefile("rb").readline()
        record = json.loads(line)
        self.assertEqual(record["schema_version"], 1)
        self.assertEqual(record["event"], "audit_consumer_connected")
        client.sendall(b"not allowed")
        deadline = time.time() + 2
        while time.time() < deadline and server.snapshot()["audit_consumers"]:
            time.sleep(0.02)
        self.assertEqual(server.snapshot()["audit_consumers"], 0)

    def test_non_loopback_bind_is_rejected(self):
        with self.assertRaises(ValueError):
            audit.AuditServer("0.0.0.0", self.free_port()).start()

    def test_bounded_queue_drops_without_blocking_emit(self):
        server = audit.AuditServer("127.0.0.1", self.free_port(), queue_size=1)
        left, right = socket.socketpair()
        self.addCleanup(left.close)
        self.addCleanup(right.close)
        consumer = audit.AuditConsumer(1, left, ("local", 1), queue.Queue(1))
        server.consumers[1] = consumer
        server.emit(audit.audit_record("one"))
        started = time.monotonic()
        server.emit(audit.audit_record("two"))
        self.assertLess(time.monotonic() - started, 0.1)
        self.assertEqual(server.events_dropped, 1)


@unittest.skipIf(audit.mesh_pb2 is None, "Meshtastic protobuf package unavailable")
class AuditIntegrationTests(unittest.TestCase):
    def free_port(self):
        probe = socket.socket()
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
        probe.close()
        return port

    def test_outbound_direct_packet_is_audited_once_not_broadcast_to_normal_client(self):
        radio_listener = socket.socket()
        radio_listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        radio_listener.bind(("127.0.0.1", 0))
        radio_listener.listen()
        radio_listener.settimeout(0.2)
        received = []
        radio_stop = threading.Event()

        def radio_loop():
            conn = None
            parser = mux.FrameParser("audit-radio")
            buf = bytearray()
            try:
                while not radio_stop.is_set():
                    if conn is None:
                        try:
                            conn, _addr = radio_listener.accept()
                            conn.settimeout(0.1)
                        except socket.timeout:
                            continue
                    try:
                        data = conn.recv(4096)
                        if data:
                            received.extend(f.payload for f in parser.feed(buf, data))
                    except socket.timeout:
                        continue
            finally:
                if conn:
                    conn.close()

        radio_thread = threading.Thread(target=radio_loop)
        radio_thread.start()
        old = (
            mux.REAL_NODE_HOST,
            mux.REAL_NODE_PORT,
            mux.LISTEN_HOST,
            mux.LISTEN_PORT,
            mux.AUDIT_ENABLED,
            mux.AUDIT_HOST,
            mux.AUDIT_PORT,
            mux.FILTER_CLIENT_ADMIN,
        )
        mux.REAL_NODE_HOST = "127.0.0.1"
        mux.REAL_NODE_PORT = radio_listener.getsockname()[1]
        mux.LISTEN_HOST = "127.0.0.1"
        mux.LISTEN_PORT = self.free_port()
        mux.AUDIT_ENABLED = True
        mux.AUDIT_HOST = "127.0.0.1"
        mux.AUDIT_PORT = self.free_port()
        mux.FILTER_CLIENT_ADMIN = False
        service = mux.MeshtasticTcpMux()
        service_thread = threading.Thread(target=service.run)
        service_thread.start()
        sockets = []
        try:
            deadline = time.time() + 2
            while time.time() < deadline:
                try:
                    sender = socket.create_connection(("127.0.0.1", mux.LISTEN_PORT), timeout=0.2)
                    observer = socket.create_connection(("127.0.0.1", mux.LISTEN_PORT), timeout=0.2)
                    audit_client = socket.create_connection(("127.0.0.1", mux.AUDIT_PORT), timeout=0.2)
                    sockets.extend((sender, observer, audit_client))
                    break
                except OSError:
                    time.sleep(0.02)
            self.assertEqual(len(sockets), 3)
            observer.settimeout(0.2)
            audit_file = audit_client.makefile("rb")
            message = audit.mesh_pb2.ToRadio()
            packet = message.packet
            setattr(packet, "from", 0xA2E9F268)
            packet.to = 0xA0352614
            packet.id = 0x10203040
            packet.decoded.portnum = audit.portnums_pb2.TEXT_MESSAGE_APP
            packet.decoded.payload = b"PRIVATE BODY"
            payload = message.SerializeToString()
            sender.sendall(mux.pack_frame(payload))

            records = []
            deadline = time.time() + 2
            while time.time() < deadline:
                line = audit_file.readline()
                if not line:
                    break
                record = json.loads(line)
                records.append(record)
                if record.get("event") == "forward_result":
                    break
            outbound = [r for r in records if r.get("event") == "client_frame"]
            self.assertEqual(len(outbound), 1)
            self.assertEqual(outbound[0]["packet_id"], 0x10203040)
            self.assertEqual(outbound[0]["from_id"], "!a2e9f268")
            self.assertEqual(outbound[0]["to_id"], "!a0352614")
            self.assertEqual(outbound[0]["application"], "TEXT_MESSAGE_APP")
            self.assertNotIn("PRIVATE BODY", json.dumps(records))
            with self.assertRaises(socket.timeout):
                observer.recv(1)
            deadline = time.time() + 1
            while time.time() < deadline and payload not in received:
                time.sleep(0.02)
            self.assertIn(payload, received)
        finally:
            for sock in sockets:
                try:
                    sock.close()
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
                mux.AUDIT_ENABLED,
                mux.AUDIT_HOST,
                mux.AUDIT_PORT,
                mux.FILTER_CLIENT_ADMIN,
            ) = old


if __name__ == "__main__":
    unittest.main()
