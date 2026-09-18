# CHAOS MUX diagnosis and repair

## Proven failure mechanism

The production daemon was version 0.2.0 with site-specific host settings; the
authoritative repository was v0.2.2. Apart from version/contact text and those
settings, the runtime implementation was identical.

The recurring localhost client was `lzfeeder.service`, which opens a Meshtastic
`TCPInterface` on port 4405 for every collection cycle. Meshtastic's stream
client sends `ToRadio.want_config_id` during construction and
`ToRadio.disconnect` during `close()`. The old MUX forwarded both messages to
the shared upstream. Journal timestamps show the upstream closing at the exact
second each feeder cycle completed and its context manager began closing. This
is why a new downstream connection appeared to cause an upstream close about
13 seconds later: 13 seconds was the feeder's processing time, not a socket
timeout. Passive clients did not send these commands and were not the cause.

While the upstream was down, the old outbound worker immediately dropped queued
frames. That accounts for pending application sends even when aggregate status
later showed few or no drops.

Three independent defects increased impact:

1. Broadcast and cache replay called `send()` synchronously. A slow client or a
   full upstream send buffer could hold a worker and locks indefinitely.
2. Watchdog notification was coupled to a health snapshot that acquired those
   locks. The observed delayed status lines followed by 60-second watchdog
   kills are consistent with this path.
3. The admin filter checked nonexistent top-level `ToRadio.admin`,
   `set_config`, and `set_module_config` fields. Real administration is a
   `ToRadio.packet.decoded` payload on `ADMIN_APP`. With protobuf missing and
   `ALLOW_RAW_WHEN_PROTOBUF_MISSING=True`, the configured filter allowed all
   traffic.

The 512-frame rolling cache also replayed transient packets, not just state. It
could duplicate application events, filled the listener thread synchronously,
and did not actually virtualize per-client configuration IDs. It is now off by
default.

## Repairs

- Treat downstream `disconnect` as local session control and never forward it.
- Decode and block actual `ADMIN_APP` packets; fail closed if enabled filters
  cannot classify a frame.
- Queue downstream writes with a per-client byte cap and disconnect only the
  slow consumer.
- Bound cache replay by frames and bytes and keep it off by default.
- Give upstream writes a deadline without changing the reader's socket mode.
- Hold outbound frames briefly across reconnects, with an age limit; never
  retry a write whose completion is ambiguous.
- Notify the systemd watchdog from a minimal thread that does not acquire MUX
  data-path locks.
- Install Meshtastic protobuf support in a dedicated virtual environment.

## Deployment

Before production deployment, create timestamped copies of both
`/opt/meshtastic-tcp-mux` and
`/etc/systemd/system/meshtastic-tcp-mux.service`. Record the current SHA-256 and
effective configuration. Run the repository tests, then use
`sudo ./install.sh --mode upgrade`; it preserves recognized site settings and
also creates an application backup. Verify the venv can import
`meshtastic.protobuf.mesh_pb2` before restarting.

Expected interruption is one service restart (normally several seconds). After
restart, confirm one upstream socket, the MeshMonitor and CHAOS downstream
sockets, watchdog status, and no upstream close when `lzfeeder` completes a
cycle. Use only controlled application test packets.

## Rollback

Stop the service, restore the timestamped application directory and unit file,
run `systemctl daemon-reload`, and start the service. This restores the previous
binary/configuration exactly. The old build forwards downstream disconnects, so
rollback also restores the diagnosed instability; it is for emergency recovery
only.
