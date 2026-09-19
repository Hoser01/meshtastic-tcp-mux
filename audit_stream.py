"""Read-only, metadata-only NDJSON observability stream for the TCP MUX."""

from __future__ import annotations

import json
import logging
import queue
import select
import socket
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple

try:
    from meshtastic.protobuf import mesh_pb2, portnums_pb2  # type: ignore
except Exception:  # pragma: no cover - optional at import time
    mesh_pb2 = None
    portnums_pb2 = None


SCHEMA_VERSION = 1


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def node_id(value: int) -> str:
    return f"!{value & 0xffffffff:08x}"


def _enum_name(wrapper: object, value: int) -> Optional[str]:
    try:
        return str(wrapper.Name(value))  # type: ignore[attr-defined]
    except Exception:
        return None


def packet_metadata(packet: object) -> Dict[str, object]:
    """Return header/transport metadata only; never include decoded payloads."""
    source = int(getattr(packet, "from"))
    destination = int(getattr(packet, "to"))
    packet_id = int(packet.id)
    result: Dict[str, object] = {
        "packet_id": packet_id,
        "from_node": source,
        "from_id": node_id(source),
        "to_node": destination,
        "to_id": node_id(destination),
        "channel": int(packet.channel),
        "hop_limit": int(packet.hop_limit),
        "hop_start": int(packet.hop_start),
        "want_ack": bool(packet.want_ack),
        "via_mqtt": bool(packet.via_mqtt),
        "encrypted": bool(packet.HasField("encrypted")),
        "correlation_id": f"packet:{source & 0xffffffff:08x}:{packet_id & 0xffffffff:08x}",
    }
    transport = int(packet.transport_mechanism)
    if transport:
        result["transport"] = _enum_name(packet.TransportMechanism, transport) or transport
    if packet.HasField("decoded"):
        portnum = int(packet.decoded.portnum)
        result["port_number"] = portnum
        if portnums_pb2 is not None:
            result["application"] = _enum_name(portnums_pb2.PortNum, portnum) or portnum
        if packet.decoded.request_id:
            result["request_id"] = int(packet.decoded.request_id)
        if packet.decoded.reply_id:
            result["reply_id"] = int(packet.decoded.reply_id)
    fields = {field.name for field, _value in packet.ListFields()}
    if "rx_rssi" in fields:
        result["rssi"] = int(packet.rx_rssi)
    if "rx_snr" in fields:
        result["snr"] = float(packet.rx_snr)
    return result


def decode_frame(direction: str, payload: bytes) -> Tuple[Dict[str, object], Optional[object]]:
    """Decode safe metadata and return the parsed protobuf when available."""
    if mesh_pb2 is None:
        return {"decode_status": "protobuf_unavailable"}, None
    try:
        message = mesh_pb2.ToRadio() if direction == "client_to_radio" else mesh_pb2.FromRadio()
        message.ParseFromString(payload)
        fields = [field.name for field, _value in message.ListFields()]
        result: Dict[str, object] = {"frame_fields": fields, "decode_status": "decoded"}
        if message.HasField("packet"):
            result.update(packet_metadata(message.packet))
        if direction == "radio_to_clients" and message.HasField("queueStatus"):
            status = message.queueStatus
            result["queue"] = {
                "result": int(status.res),
                "free": int(status.free),
                "maxlen": int(status.maxlen),
                "mesh_packet_id": int(status.mesh_packet_id),
            }
        return result, message
    except Exception as exc:
        return {"decode_status": "malformed", "error": exc.__class__.__name__}, None


def audit_record(
    event: str,
    direction: Optional[str] = None,
    disposition: Optional[str] = None,
    **values: object,
) -> Dict[str, object]:
    record: Dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "event": event,
        "observed_at": utc_now(),
    }
    if direction is not None:
        record["direction"] = direction
    if disposition is not None:
        record["disposition"] = disposition
    record.update({key: value for key, value in values.items() if value is not None})
    return record


@dataclass
class AuditConsumer:
    cid: int
    sock: socket.socket
    addr: Tuple[str, int]
    events: "queue.Queue[bytes]"
    dropped: int = 0
    stop: threading.Event = field(default_factory=threading.Event)


class AuditServer:
    """Fan out sanitized records without blocking the MUX data path."""

    def __init__(
        self,
        host: str,
        port: int,
        queue_size: int = 1000,
        max_consumers: int = 4,
        send_timeout: float = 1.0,
    ) -> None:
        self.host = host
        self.port = port
        self.queue_size = queue_size
        self.max_consumers = max_consumers
        self.send_timeout = send_timeout
        self.stop_event = threading.Event()
        self.consumers: Dict[int, AuditConsumer] = {}
        self.lock = threading.Lock()
        self.listener: Optional[socket.socket] = None
        self.thread: Optional[threading.Thread] = None
        self._next_id = 1
        self.events_emitted = 0
        self.events_dropped = 0
        self._last_drop_warning = 0.0

    def start(self) -> None:
        if self.host not in ("127.0.0.1", "localhost"):
            raise ValueError("audit host must be loopback")
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((self.host, self.port))
        listener.listen(self.max_consumers)
        listener.settimeout(0.5)
        self.listener = listener
        self.thread = threading.Thread(target=self._accept_loop, name="audit", daemon=True)
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        if self.listener is not None:
            try:
                self.listener.close()
            except OSError:
                pass
        with self.lock:
            consumers = list(self.consumers.values())
        for consumer in consumers:
            self._remove(consumer)
        if self.thread is not None:
            self.thread.join(timeout=2)

    def emit(self, record: Dict[str, object]) -> None:
        try:
            encoded = json.dumps(record, separators=(",", ":"), sort_keys=True).encode("utf-8") + b"\n"
        except (TypeError, ValueError):
            return
        self.events_emitted += 1
        with self.lock:
            consumers = list(self.consumers.values())
        dropped_ids = []
        for consumer in consumers:
            try:
                consumer.events.put_nowait(encoded)
            except queue.Full:
                consumer.dropped += 1
                self.events_dropped += 1
                dropped_ids.append(consumer.cid)
                now = time.monotonic()
                if now - self._last_drop_warning >= 60:
                    logging.warning("audit consumer queue full; dropping metadata events")
                    self._last_drop_warning = now
        if dropped_ids:
            notice = json.dumps(
                audit_record(
                    "audit_queue_drop",
                    disposition="dropped",
                    affected_consumers=dropped_ids,
                    total_dropped=self.events_dropped,
                ),
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8") + b"\n"
            for consumer in consumers:
                if consumer.cid not in dropped_ids:
                    try:
                        consumer.events.put_nowait(notice)
                    except queue.Full:
                        pass

    def snapshot(self) -> Dict[str, int]:
        with self.lock:
            count = len(self.consumers)
        return {
            "audit_consumers": count,
            "audit_events": self.events_emitted,
            "audit_dropped": self.events_dropped,
        }

    def _accept_loop(self) -> None:
        listener = self.listener
        if listener is None:
            return
        logging.info("audit stream listening on %s:%d", self.host, self.port)
        try:
            while not self.stop_event.is_set():
                try:
                    sock, addr = listener.accept()
                except socket.timeout:
                    continue
                except OSError:
                    break
                with self.lock:
                    if len(self.consumers) >= self.max_consumers:
                        sock.close()
                        continue
                    cid = self._next_id
                    self._next_id += 1
                    consumer = AuditConsumer(cid, sock, addr, queue.Queue(self.queue_size))
                    self.consumers[cid] = consumer
                threading.Thread(
                    target=self._consumer_loop,
                    args=(consumer,),
                    name=f"audit-{cid}",
                    daemon=True,
                ).start()
                self.emit(audit_record("audit_consumer_connected", audit_consumer_id=cid))
        finally:
            try:
                listener.close()
            except OSError:
                pass
            self.listener = None

    def _consumer_loop(self, consumer: AuditConsumer) -> None:
        consumer.sock.setblocking(False)
        reason = "disconnected"
        try:
            while not self.stop_event.is_set() and not consumer.stop.is_set():
                readable, _writable, _errors = select.select([consumer.sock], [], [consumer.sock], 0)
                if readable:
                    incoming = consumer.sock.recv(1, socket.MSG_PEEK)
                    if incoming:
                        reason = "consumer_write_rejected"
                        break
                    if incoming == b"":
                        break
                try:
                    data = consumer.events.get(timeout=0.25)
                except queue.Empty:
                    continue
                self._send(consumer.sock, data)
        except (OSError, TimeoutError):
            reason = "send_failure"
        finally:
            self._remove(consumer)
            self.emit(
                audit_record(
                    "audit_consumer_disconnected",
                    audit_consumer_id=consumer.cid,
                    reason=reason,
                    dropped_events=consumer.dropped,
                )
            )

    def _send(self, sock: socket.socket, data: bytes) -> None:
        view = memoryview(data)
        deadline = time.monotonic() + self.send_timeout
        while view:
            try:
                sent = sock.send(view)
                if sent <= 0:
                    raise ConnectionError("audit socket closed")
                view = view[sent:]
            except BlockingIOError:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("audit send timeout")
                _readable, writable, _errors = select.select([], [sock], [sock], remaining)
                if not writable:
                    raise TimeoutError("audit send timeout")

    def _remove(self, consumer: AuditConsumer) -> None:
        consumer.stop.set()
        with self.lock:
            self.consumers.pop(consumer.cid, None)
        try:
            consumer.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            consumer.sock.close()
        except OSError:
            pass
