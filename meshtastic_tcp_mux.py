#!/usr/bin/env python3
"""
meshtastic-tcp-mux

Small TCP fanout/mux for Meshtastic's stream protocol.
One upstream node connection, many local downstream TCP clients.

Contact: Hoser/Chris de W0WC at info@larc.com for comments, suggestions, or issues.
"""

from __future__ import annotations

import argparse
import errno
import logging
import os
import queue
import selectors
import signal
import socket
import struct
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, Iterable, List, Optional, Tuple


# =============================================================================
# Configuration
# =============================================================================

REAL_NODE_HOST = "192.168.86.130"
REAL_NODE_PORT = 4403

LISTEN_HOST = "0.0.0.0"
LISTEN_PORT = 4405

MAX_CLIENTS = 12
CLIENT_IDLE_TIMEOUT_SECONDS = 0        # 0 disables idle timeout
CLIENT_RECV_BUFFER = 4096
UPSTREAM_RECV_BUFFER = 4096

RECONNECT_DELAY_SECONDS = 5
CONNECT_TIMEOUT_SECONDS = 8
SOCKET_KEEPALIVE = True

OUTBOUND_DELAY_SECONDS = 0.35          # spacing between client-originated frames
OUTBOUND_QUEUE_SIZE = 500
DROP_CLIENT_IF_QUEUE_FULL = False

CACHE_REPLAY_TO_NEW_CLIENTS = True
CACHE_MAX_FRAMES = 512
CACHE_MAX_AGE_SECONDS = 900            # 15 minutes

FILTER_CLIENT_ADMIN = True             # requires meshtastic protobuf package
FILTER_CLIENT_CONFIG = False           # if True, blocks client config writes too
FILTER_CLIENT_MODULE_CONFIG = False    # if True, blocks module config writes too
ALLOW_RAW_WHEN_PROTOBUF_MISSING = True  # if False, block filtered packet types when protobuf parsing unavailable

LOG_LEVEL = "INFO"
LOG_HEX_FRAMES = False
LOG_FRAME_SUMMARY = True

# Meshtastic stream framing used by the Python StreamInterface.
# Header is START1, START2, length MSB, length LSB, then protobuf payload.
START1 = 0x94
START2 = 0xC3
ALT_START2_VALUES = (0x93,)            # tolerated on input only; output uses START2
HEADER_LEN = 4
MAX_FRAME_SIZE = 512

SERVICE_NAME = "meshtastic-tcp-mux"
VERSION = "0.2.1"

HEALTH_CHECK_INTERVAL_SECONDS = 15
LISTENER_RESTART_DELAY_SECONDS = 2
LISTENER_MAX_CONSECUTIVE_FAILURES = 3
SYSTEMD_WATCHDOG_ENABLED = True


# =============================================================================
# Optional protobuf support
# =============================================================================

try:
    from meshtastic.protobuf import mesh_pb2  # type: ignore
except Exception:  # pragma: no cover - intentionally broad for optional dependency
    mesh_pb2 = None


# =============================================================================
# Data structures
# =============================================================================

@dataclass
class Frame:
    payload: bytes
    raw: bytes
    received_at: float = field(default_factory=time.time)


@dataclass
class Client:
    sock: socket.socket
    addr: Tuple[str, int]
    cid: int
    rxbuf: bytearray = field(default_factory=bytearray)
    connected_at: float = field(default_factory=time.time)
    last_seen: float = field(default_factory=time.time)
    frames_in: int = 0
    frames_out: int = 0
    bytes_in: int = 0
    bytes_out: int = 0

    @property
    def name(self) -> str:
        return f"client-{self.cid} {self.addr[0]}:{self.addr[1]}"


@dataclass
class OutboundItem:
    client_id: int
    client_addr: Tuple[str, int]
    frame: Frame


class Stats:
    def __init__(self) -> None:
        self.started_at = time.time()
        self.upstream_connects = 0
        self.upstream_disconnects = 0
        self.frames_from_node = 0
        self.frames_to_node = 0
        self.frames_from_clients = 0
        self.frames_to_clients = 0
        self.frames_dropped = 0
        self.frames_blocked = 0
        self.client_connects = 0
        self.client_disconnects = 0
        self.lock = threading.Lock()

    def inc(self, field_name: str, amount: int = 1) -> None:
        with self.lock:
            setattr(self, field_name, getattr(self, field_name) + amount)

    def snapshot(self) -> Dict[str, int | float]:
        with self.lock:
            return {
                "uptime_seconds": round(time.time() - self.started_at, 1),
                "upstream_connects": self.upstream_connects,
                "upstream_disconnects": self.upstream_disconnects,
                "frames_from_node": self.frames_from_node,
                "frames_to_node": self.frames_to_node,
                "frames_from_clients": self.frames_from_clients,
                "frames_to_clients": self.frames_to_clients,
                "frames_dropped": self.frames_dropped,
                "frames_blocked": self.frames_blocked,
                "client_connects": self.client_connects,
                "client_disconnects": self.client_disconnects,
            }


# =============================================================================
# Framing
# =============================================================================

class FrameParser:
    def __init__(self, label: str) -> None:
        self.label = label

    def feed(self, buf: bytearray, data: bytes) -> List[Frame]:
        if data:
            buf.extend(data)
        frames: List[Frame] = []

        while True:
            if len(buf) < HEADER_LEN:
                break

            start_index = self._find_start(buf)
            if start_index < 0:
                if buf:
                    logging.debug("%s: discarding %d bytes while seeking frame start", self.label, len(buf))
                    del buf[:]
                break

            if start_index > 0:
                logging.debug("%s: discarding %d stray bytes", self.label, start_index)
                del buf[:start_index]

            if len(buf) < HEADER_LEN:
                break

            length = (buf[2] << 8) | buf[3]
            if length < 0 or length > MAX_FRAME_SIZE:
                logging.warning("%s: invalid frame length %d, resyncing", self.label, length)
                del buf[0]
                continue

            needed = HEADER_LEN + length
            if len(buf) < needed:
                break

            raw = bytes(buf[:needed])
            payload = bytes(buf[HEADER_LEN:needed])
            del buf[:needed]
            frames.append(Frame(payload=payload, raw=raw))

        return frames

    @staticmethod
    def _find_start(buf: bytearray) -> int:
        valid_second = (START2, *ALT_START2_VALUES)
        for idx in range(0, max(0, len(buf) - 1)):
            if buf[idx] == START1 and buf[idx + 1] in valid_second:
                return idx
        return -1


def pack_frame(payload: bytes) -> bytes:
    if len(payload) > MAX_FRAME_SIZE:
        raise ValueError(f"payload too large: {len(payload)} > {MAX_FRAME_SIZE}")
    return bytes((START1, START2)) + struct.pack(">H", len(payload)) + payload


def normalize_frame(frame: Frame) -> Frame:
    raw = pack_frame(frame.payload)
    if raw == frame.raw:
        return frame
    return Frame(payload=frame.payload, raw=raw, received_at=frame.received_at)


# =============================================================================
# Protobuf filtering / summaries
# =============================================================================

def summarize_payload(direction: str, payload: bytes) -> str:
    if not LOG_FRAME_SUMMARY:
        return ""
    if mesh_pb2 is None:
        return f"{direction} len={len(payload)}"

    try:
        if direction == "to_radio":
            msg = mesh_pb2.ToRadio()
        else:
            msg = mesh_pb2.FromRadio()
        msg.ParseFromString(payload)
        fields = [name for name, _value in msg.ListFields()]
        if not fields:
            return f"{direction} len={len(payload)} empty"
        return f"{direction} len={len(payload)} fields={','.join(fields)}"
    except Exception as exc:
        return f"{direction} len={len(payload)} parse_error={exc.__class__.__name__}"


def client_frame_allowed(frame: Frame) -> Tuple[bool, str]:
    if not (FILTER_CLIENT_ADMIN or FILTER_CLIENT_CONFIG or FILTER_CLIENT_MODULE_CONFIG):
        return True, "allowed"

    if mesh_pb2 is None:
        if ALLOW_RAW_WHEN_PROTOBUF_MISSING:
            return True, "protobuf unavailable, raw allowed"
        return False, "protobuf unavailable"

    try:
        msg = mesh_pb2.ToRadio()
        msg.ParseFromString(frame.payload)
        field_names = {name for name, _value in msg.ListFields()}
    except Exception as exc:
        if ALLOW_RAW_WHEN_PROTOBUF_MISSING:
            return True, f"unparsed protobuf allowed: {exc.__class__.__name__}"
        return False, f"unparsed protobuf blocked: {exc.__class__.__name__}"

    blocked: List[str] = []
    if FILTER_CLIENT_ADMIN and "admin" in field_names:
        blocked.append("admin")
    if FILTER_CLIENT_CONFIG and "set_config" in field_names:
        blocked.append("set_config")
    if FILTER_CLIENT_MODULE_CONFIG and "set_module_config" in field_names:
        blocked.append("set_module_config")

    if blocked:
        return False, "blocked fields: " + ",".join(blocked)
    return True, "allowed"


# =============================================================================
# Socket helpers
# =============================================================================

def set_common_sockopts(sock: socket.socket) -> None:
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    if SOCKET_KEEPALIVE:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)


def close_quietly(sock: socket.socket) -> None:
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except Exception:
        pass
    try:
        sock.close()
    except Exception:
        pass


def send_all(sock: socket.socket, data: bytes) -> None:
    view = memoryview(data)
    while view:
        sent = sock.send(view)
        if sent == 0:
            raise ConnectionError("socket send returned 0")
        view = view[sent:]


# =============================================================================
# Mux service
# =============================================================================

class MeshtasticTcpMux:
    def __init__(self) -> None:
        self.stop_event = threading.Event()
        self.exit_event = threading.Event()
        self.exit_code = 0
        self.outbound: queue.Queue[OutboundItem] = queue.Queue(maxsize=OUTBOUND_QUEUE_SIZE)
        self.clients: Dict[int, Client] = {}
        self.clients_lock = threading.RLock()
        self.selector_lock = threading.RLock()
        self.cache: Deque[Frame] = deque(maxlen=CACHE_MAX_FRAMES)
        self.cache_lock = threading.Lock()
        self.stats = Stats()
        self._next_client_id = 1
        self._upstream_sock: Optional[socket.socket] = None
        self._upstream_lock = threading.Lock()
        self._upstream_state = "starting"
        self._listen_sock: Optional[socket.socket] = None
        self._listener_selector: Optional[selectors.BaseSelector] = None
        self._listener_thread: Optional[threading.Thread] = None
        self._upstream_thread: Optional[threading.Thread] = None
        self._outbound_thread: Optional[threading.Thread] = None
        self._status_thread: Optional[threading.Thread] = None
        self._health_thread: Optional[threading.Thread] = None
        self._thread_lock = threading.RLock()

    def run(self) -> int:
        self._print_startup()

        self._upstream_thread = self._start_thread("upstream", self._upstream_loop)
        self._start_listener_thread()
        self._outbound_thread = self._start_thread("outbound", self._outbound_loop)
        self._status_thread = self._start_thread("status", self._status_loop)
        self._health_thread = self._start_thread("health", self._health_loop)

        try:
            while not self.stop_event.is_set() and not self.exit_event.is_set():
                time.sleep(0.25)
        except KeyboardInterrupt:
            logging.info("interrupt received")
        finally:
            self.stop()
            for thread in self._all_threads():
                thread.join(timeout=3)

        logging.info("stopped")
        return self.exit_code

    def _start_thread(self, name: str, target: object) -> threading.Thread:
        thread = threading.Thread(target=target, name=name, daemon=True)
        thread.start()
        return thread

    def _all_threads(self) -> List[threading.Thread]:
        with self._thread_lock:
            return [
                thread
                for thread in (
                    self._upstream_thread,
                    self._listener_thread,
                    self._outbound_thread,
                    self._status_thread,
                    self._health_thread,
                )
                if thread is not None
            ]

    def _start_listener_thread(self) -> bool:
        with self._thread_lock:
            if self._listener_thread is not None and self._listener_thread.is_alive():
                return True
            logging.info("listener thread started")
            self._listener_thread = self._start_thread("listener", self._server_loop_supervisor)
            return True

    def _request_exit(self, code: int, reason: str) -> None:
        if not self.exit_event.is_set():
            logging.error("exiting for systemd restart: %s", reason)
        self.exit_code = code
        self.exit_event.set()
        self.stop_event.set()

    def stop(self) -> None:
        self.stop_event.set()
        with self._upstream_lock:
            if self._upstream_sock is not None:
                close_quietly(self._upstream_sock)
                self._upstream_sock = None
            self._upstream_state = "stopped"
        with self.selector_lock:
            selector = self._listener_selector
            self._close_all_clients(selector, "service stopping")
            if self._listen_sock is not None:
                close_quietly(self._listen_sock)
                self._listen_sock = None

    def _print_startup(self) -> None:
        logging.info("%s starting", SERVICE_NAME)
        logging.info("upstream node: %s:%d", REAL_NODE_HOST, REAL_NODE_PORT)
        logging.info("listen: %s:%d", LISTEN_HOST, LISTEN_PORT)
        logging.info("max clients: %d", MAX_CLIENTS)
        logging.info("cache replay: %s", "on" if CACHE_REPLAY_TO_NEW_CLIENTS else "off")
        logging.info("client admin filter: %s", "on" if FILTER_CLIENT_ADMIN else "off")
        if mesh_pb2 is None:
            logging.warning("meshtastic protobuf package not available; packet filtering/summaries are limited")

    def _upstream_loop(self) -> None:
        parser = FrameParser("upstream")
        rxbuf = bytearray()

        while not self.stop_event.is_set():
            sock: Optional[socket.socket] = None
            try:
                with self._upstream_lock:
                    self._upstream_state = "reconnecting"
                sock = self._connect_upstream()
                with self._upstream_lock:
                    self._upstream_sock = sock
                    self._upstream_state = "connected"
                self.stats.inc("upstream_connects")
                rxbuf.clear()

                while not self.stop_event.is_set():
                    data = sock.recv(UPSTREAM_RECV_BUFFER)
                    if not data:
                        raise ConnectionError("upstream closed")
                    for frame in parser.feed(rxbuf, data):
                        self._handle_upstream_frame(frame)

            except Exception as exc:
                if not self.stop_event.is_set():
                    logging.warning("upstream disconnected: %s", exc)
                self.stats.inc("upstream_disconnects")
            finally:
                with self._upstream_lock:
                    if self._upstream_sock is sock:
                        self._upstream_sock = None
                    if not self.stop_event.is_set():
                        self._upstream_state = "reconnecting"
                if sock is not None:
                    close_quietly(sock)

            if not self.stop_event.is_set():
                time.sleep(RECONNECT_DELAY_SECONDS)

        with self._upstream_lock:
            self._upstream_state = "stopped"

    def _connect_upstream(self) -> socket.socket:
        logging.info("connecting to upstream %s:%d", REAL_NODE_HOST, REAL_NODE_PORT)
        sock = socket.create_connection((REAL_NODE_HOST, REAL_NODE_PORT), timeout=CONNECT_TIMEOUT_SECONDS)
        set_common_sockopts(sock)
        sock.settimeout(None)
        logging.info("connected to upstream")
        return sock

    def _handle_upstream_frame(self, frame: Frame) -> None:
        frame = normalize_frame(frame)
        self.stats.inc("frames_from_node")
        self._cache_frame(frame)

        summary = summarize_payload("from_radio", frame.payload)
        if summary:
            logging.debug("node -> clients: %s", summary)
        if LOG_HEX_FRAMES:
            logging.debug("node frame: %s", frame.raw.hex())

        self._broadcast(frame.raw)

    def _cache_frame(self, frame: Frame) -> None:
        if not CACHE_REPLAY_TO_NEW_CLIENTS:
            return
        with self.cache_lock:
            self.cache.append(frame)

    def _server_loop_supervisor(self) -> None:
        failures = 0
        while not self.stop_event.is_set():
            try:
                self._server_loop()
                failures = 0
            except Exception:
                failures += 1
                logging.exception("listener thread crashed")
                if failures >= LISTENER_MAX_CONSECUTIVE_FAILURES:
                    self._request_exit(1, "listener failed repeatedly")
                    break
                logging.info("restarting listener thread")
                time.sleep(LISTENER_RESTART_DELAY_SECONDS)
        logging.info("listener thread stopped")

    def _server_loop(self) -> None:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        set_common_sockopts(server)
        server.bind((LISTEN_HOST, LISTEN_PORT))
        server.listen(MAX_CLIENTS)
        server.setblocking(False)

        selector = selectors.DefaultSelector()
        self._safe_register(selector, server, selectors.EVENT_READ, "server")
        parser = FrameParser("client")

        with self.selector_lock:
            self._listen_sock = server
            self._listener_selector = selector

        logging.info("listen socket bound to %s:%d", LISTEN_HOST, LISTEN_PORT)

        try:
            while not self.stop_event.is_set():
                with self.selector_lock:
                    events = selector.select(timeout=0.5)
                for key, _mask in events:
                    if key.data == "server":
                        self._accept_client(server, selector)
                    else:
                        client: Client = key.data
                        self._read_client(client, selector, parser)
                self._drop_idle_clients(selector)
        finally:
            logging.info("listener loop cleaning up")
            with self.selector_lock:
                self._close_all_clients(selector, "listener restarting")
                self._safe_unregister(selector, server)
                close_quietly(server)
                if self._listen_sock is server:
                    self._listen_sock = None
                if self._listener_selector is selector:
                    self._listener_selector = None
                selector.close()

    def _safe_register(
        self,
        selector: selectors.BaseSelector,
        sock: socket.socket,
        events: int,
        data: object,
    ) -> None:
        with self.selector_lock:
            try:
                selector.get_key(sock)
                logging.warning("socket already registered, unregistering first")
                selector.unregister(sock)
            except KeyError:
                pass
            selector.register(sock, events, data=data)

    def _safe_unregister(self, selector: selectors.BaseSelector, sock: socket.socket) -> None:
        try:
            selector.unregister(sock)
        except KeyError:
            pass
        except Exception as exc:
            logging.debug("socket unregister failed: %s", exc)

    def _accept_client(self, server: socket.socket, selector: selectors.BaseSelector) -> None:
        try:
            sock, addr = server.accept()
        except OSError as exc:
            if exc.errno not in (errno.EAGAIN, errno.EWOULDBLOCK):
                logging.warning("accept failed: %s", exc)
            return

        with self.clients_lock:
            if len(self.clients) >= MAX_CLIENTS:
                logging.warning("rejecting client %s:%d; max clients reached", addr[0], addr[1])
                close_quietly(sock)
                return

            cid = self._next_client_id
            self._next_client_id += 1
            client = Client(sock=sock, addr=addr, cid=cid)
            self.clients[cid] = client

        set_common_sockopts(sock)
        sock.setblocking(False)
        try:
            self._safe_register(selector, sock, selectors.EVENT_READ, client)
        except Exception:
            with self.clients_lock:
                self.clients.pop(client.cid, None)
            close_quietly(sock)
            logging.exception("failed registering %s", client.name)
            raise
        self.stats.inc("client_connects")
        logging.info("%s connected", client.name)

        if CACHE_REPLAY_TO_NEW_CLIENTS:
            self._replay_cache(client, selector)

    def _read_client(self, client: Client, selector: selectors.BaseSelector, parser: FrameParser) -> None:
        try:
            data = client.sock.recv(CLIENT_RECV_BUFFER)
            if not data:
                self._disconnect_client(client, selector, "closed")
                return

            client.last_seen = time.time()
            client.bytes_in += len(data)

            for frame in parser.feed(client.rxbuf, data):
                self._handle_client_frame(client, selector, frame)

        except ConnectionError as exc:
            self._disconnect_client(client, selector, str(exc))
        except OSError as exc:
            self._disconnect_client(client, selector, str(exc))

    def _handle_client_frame(self, client: Client, selector: selectors.BaseSelector, frame: Frame) -> None:
        frame = normalize_frame(frame)
        client.frames_in += 1
        self.stats.inc("frames_from_clients")

        allowed, reason = client_frame_allowed(frame)
        summary = summarize_payload("to_radio", frame.payload)

        if not allowed:
            self.stats.inc("frames_blocked")
            logging.warning("%s blocked: %s (%s)", client.name, reason, summary)
            return

        if summary:
            logging.debug("%s -> node: %s", client.name, summary)
        if LOG_HEX_FRAMES:
            logging.debug("%s frame: %s", client.name, frame.raw.hex())

        item = OutboundItem(client_id=client.cid, client_addr=client.addr, frame=frame)
        try:
            self.outbound.put_nowait(item)
        except queue.Full:
            self.stats.inc("frames_dropped")
            logging.warning("outbound queue full; dropping frame from %s", client.name)
            if DROP_CLIENT_IF_QUEUE_FULL:
                self._disconnect_client(client, selector, "outbound queue full")

    def _disconnect_client(
        self,
        client: Client,
        selector: Optional[selectors.BaseSelector],
        reason: str,
    ) -> None:
        with self.clients_lock:
            existing = self.clients.pop(client.cid, None)
        if existing is None:
            return
        active_selector = selector
        if active_selector is None:
            with self.selector_lock:
                active_selector = self._listener_selector
        if active_selector is not None:
            with self.selector_lock:
                self._safe_unregister(active_selector, client.sock)
        close_quietly(client.sock)
        self.stats.inc("client_disconnects")
        logging.info("%s disconnected: %s", client.name, reason)

    def _close_all_clients(self, selector: Optional[selectors.BaseSelector], reason: str) -> None:
        with self.clients_lock:
            clients = list(self.clients.values())
            self.clients.clear()
        for client in clients:
            if selector is not None:
                self._safe_unregister(selector, client.sock)
            close_quietly(client.sock)
            self.stats.inc("client_disconnects")
            logging.info("%s disconnected: %s", client.name, reason)

    def _drop_idle_clients(self, selector: selectors.BaseSelector) -> None:
        if CLIENT_IDLE_TIMEOUT_SECONDS <= 0:
            return
        now = time.time()
        with self.clients_lock:
            clients = list(self.clients.values())
        for client in clients:
            if now - client.last_seen > CLIENT_IDLE_TIMEOUT_SECONDS:
                self._disconnect_client(client, selector, "idle timeout")

    def _replay_cache(self, client: Client, selector: selectors.BaseSelector) -> None:
        cutoff = time.time() - CACHE_MAX_AGE_SECONDS
        with self.cache_lock:
            frames = [frame for frame in self.cache if frame.received_at >= cutoff]

        if not frames:
            return

        sent = 0
        try:
            for frame in frames:
                send_all(client.sock, frame.raw)
                client.frames_out += 1
                client.bytes_out += len(frame.raw)
                sent += 1
            self.stats.inc("frames_to_clients", sent)
            logging.info("replayed %d cached frames to %s", sent, client.name)
        except Exception as exc:
            logging.warning("cache replay failed for %s: %s", client.name, exc)
            self._disconnect_client(client, selector, "cache replay failed")

    def _broadcast(self, data: bytes) -> None:
        with self.clients_lock:
            clients = list(self.clients.values())

        dead: List[Client] = []
        sent = 0
        for client in clients:
            try:
                send_all(client.sock, data)
                client.frames_out += 1
                client.bytes_out += len(data)
                sent += 1
            except Exception as exc:
                logging.info("%s send failed: %s", client.name, exc)
                dead.append(client)

        if sent:
            self.stats.inc("frames_to_clients", sent)

        if dead:
            for client in dead:
                self._disconnect_client(client, None, "send failed")

    def _outbound_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                item = self.outbound.get(timeout=0.5)
            except queue.Empty:
                continue

            try:
                self._send_to_upstream(item)
                self.stats.inc("frames_to_node")
            except Exception as exc:
                self.stats.inc("frames_dropped")
                logging.warning(
                    "failed sending frame from client-%d %s:%d to upstream: %s",
                    item.client_id,
                    item.client_addr[0],
                    item.client_addr[1],
                    exc,
                )
            finally:
                if OUTBOUND_DELAY_SECONDS > 0:
                    time.sleep(OUTBOUND_DELAY_SECONDS)

    def _send_to_upstream(self, item: OutboundItem) -> None:
        with self._upstream_lock:
            sock = self._upstream_sock
            if sock is None:
                raise ConnectionError("upstream is not connected")
            send_all(sock, item.frame.raw)

    def _health_snapshot(self) -> Dict[str, object]:
        with self._thread_lock:
            listener_alive = self._listener_thread is not None and self._listener_thread.is_alive()
            upstream_alive = self._upstream_thread is not None and self._upstream_thread.is_alive()
        with self.clients_lock:
            client_count = len(self.clients)
        with self._upstream_lock:
            upstream_state = self._upstream_state
            upstream_connected = self._upstream_sock is not None
        listening = self._listen_socket_bound()
        return {
            "listener_alive": listener_alive,
            "upstream_alive": upstream_alive,
            "listening": listening,
            "client_count": client_count,
            "upstream_state": upstream_state,
            "upstream_connected": upstream_connected,
        }

    def _listen_socket_bound(self) -> bool:
        with self.selector_lock:
            sock = self._listen_sock
            if sock is None or sock.fileno() < 0:
                return False
            try:
                host, port = sock.getsockname()[:2]
            except OSError:
                return False
        return port == LISTEN_PORT and (LISTEN_HOST in ("0.0.0.0", host) or host == LISTEN_HOST)

    def _health_loop(self) -> None:
        self._systemd_notify("READY=1\nSTATUS=meshtastic-tcp-mux running")
        while not self.stop_event.wait(HEALTH_CHECK_INTERVAL_SECONDS):
            health = self._health_snapshot()
            if not health["listener_alive"]:
                logging.error("health check failed: listener thread dead")
                logging.info("restarting listener thread")
                try:
                    self._start_listener_thread()
                except Exception:
                    logging.exception("listener restart failed")
                    self._request_exit(1, "listener thread could not be restarted")
                    break
                continue

            if not health["listening"]:
                logging.error("health check failed: port %d not listening", LISTEN_PORT)
                self._request_exit(1, "listener thread alive but listen socket is not bound")
                break

            if not health["upstream_alive"]:
                logging.error("health check failed: upstream thread dead")
                self._request_exit(1, "upstream thread died")
                break

            upstream_state = str(health["upstream_state"])
            if upstream_state not in ("connected", "reconnecting"):
                logging.warning("health check warning: upstream state is %s", upstream_state)

            self._systemd_notify(
                "WATCHDOG=1\n"
                f"STATUS=listener_alive={health['listener_alive']} "
                f"upstream_alive={health['upstream_alive']} "
                f"listening={health['listening']} "
                f"client_count={health['client_count']} "
                f"upstream_state={upstream_state}"
            )

    def _systemd_notify(self, message: str) -> None:
        if not SYSTEMD_WATCHDOG_ENABLED:
            return
        notify_socket = os.environ.get("NOTIFY_SOCKET")
        if not notify_socket:
            return
        addr: str | bytes = notify_socket
        if notify_socket.startswith("@"):
            addr = "\0" + notify_socket[1:]
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        try:
            sock.connect(addr)
            sock.sendall(message.encode("utf-8"))
        except Exception as exc:
            logging.debug("systemd notify failed: %s", exc)
        finally:
            sock.close()

    def _status_loop(self) -> None:
        while not self.stop_event.is_set():
            time.sleep(60)
            if self.stop_event.is_set():
                break
            snap = self.stats.snapshot()
            health = self._health_snapshot()
            logging.info(
                "status: up=%ss listener_alive=%s upstream_alive=%s listening=%s client_count=%d upstream_state=%s node_rx=%d node_tx=%d client_rx=%d client_tx=%d blocked=%d dropped=%d",
                snap["uptime_seconds"],
                health["listener_alive"],
                health["upstream_alive"],
                health["listening"],
                health["client_count"],
                health["upstream_state"],
                snap["frames_from_node"],
                snap["frames_to_node"],
                snap["frames_from_clients"],
                snap["frames_to_clients"],
                snap["frames_blocked"],
                snap["frames_dropped"],
            )


# =============================================================================
# CLI / main
# =============================================================================

def configure_logging(level: str) -> None:
    numeric = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric,
        format="%(asctime)s %(levelname)-8s %(threadName)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def parse_args(argv: Iterable[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Meshtastic TCP fanout/mux")
    parser.add_argument("--check", action="store_true", help="show effective config and exit")
    parser.add_argument("--debug", action="store_true", help="set log level to DEBUG")
    parser.add_argument("--version", action="store_true", help="show version and exit")
    return parser.parse_args(list(argv))


def show_config() -> None:
    print(f"SERVICE_NAME={SERVICE_NAME}")
    print(f"VERSION={VERSION}")
    print(f"REAL_NODE_HOST={REAL_NODE_HOST}")
    print(f"REAL_NODE_PORT={REAL_NODE_PORT}")
    print(f"LISTEN_HOST={LISTEN_HOST}")
    print(f"LISTEN_PORT={LISTEN_PORT}")
    print(f"MAX_CLIENTS={MAX_CLIENTS}")
    print(f"CACHE_REPLAY_TO_NEW_CLIENTS={CACHE_REPLAY_TO_NEW_CLIENTS}")
    print(f"FILTER_CLIENT_ADMIN={FILTER_CLIENT_ADMIN}")
    print(f"FILTER_CLIENT_CONFIG={FILTER_CLIENT_CONFIG}")
    print(f"FILTER_CLIENT_MODULE_CONFIG={FILTER_CLIENT_MODULE_CONFIG}")
    print(f"HEALTH_CHECK_INTERVAL_SECONDS={HEALTH_CHECK_INTERVAL_SECONDS}")
    print(f"SYSTEMD_WATCHDOG_ENABLED={SYSTEMD_WATCHDOG_ENABLED}")
    print(f"START1=0x{START1:02X}")
    print(f"START2=0x{START2:02X}")
    print(f"ALT_START2_VALUES={[hex(v) for v in ALT_START2_VALUES]}")
    print(f"protobuf_available={mesh_pb2 is not None}")


def install_signal_handlers(service: MeshtasticTcpMux) -> None:
    def handler(_signum: int, _frame: object) -> None:
        logging.info("signal received, shutting down")
        service.stop()

    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGINT, handler)


def main(argv: Iterable[str] = sys.argv[1:]) -> int:
    args = parse_args(argv)
    configure_logging("DEBUG" if args.debug else LOG_LEVEL)

    if args.version:
        print(f"{SERVICE_NAME} {VERSION}")
        return 0

    if args.check:
        show_config()
        return 0

    service = MeshtasticTcpMux()
    install_signal_handlers(service)
    return service.run()


if __name__ == "__main__":
    raise SystemExit(main())
