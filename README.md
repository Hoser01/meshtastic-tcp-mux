# meshtastic-tcp-mux

A small standalone TCP multiplexer for Meshtastic.

Contact: Hoser/Chris de W0WC at info@lzarc.com for comments, suggestions, or
issues.

It keeps one TCP connection open to a real Meshtastic node, then allows multiple
local or remote clients/scripts to connect to a separate virtual TCP port.

Default layout:

```text
Real Meshtastic node: 192.168.86.130:4403
Mux listen port:      0.0.0.0:4405
```

This is useful when several tools need access to the same node without each tool
opening its own direct connection to the device.

```text
Physical Meshtastic node :4403
          |
          v
meshtastic-tcp-mux :4405
          |
          +-- script 1
          +-- script 2
          +-- bot
          +-- test client
```

## Requirements

- Linux host with `systemd`
- `python3` and `python3-venv`
- `unzip`, if installing from the release zip
- `git`, if installing from source
- Root/sudo access for installing the service under `/opt` and `/etc/systemd`
- Network reachability from the mux host to the Meshtastic TCP interface

- The Meshtastic Python package. The installer creates an isolated virtual
  environment and installs the version range in `requirements.txt`:

```bash
python3 -m venv /opt/meshtastic-tcp-mux/venv
/opt/meshtastic-tcp-mux/venv/bin/pip install -r requirements.txt
```

The daemon can start without this dependency, but when any protobuf-based
filter is enabled it fails closed for client frames it cannot classify. This
prevents an apparently enabled admin filter from silently allowing admin
traffic.

## Install from Release Zip

Upload the versioned release zip file to `/tmp` on the Linux machine.

```bash
cd /tmp
unzip meshtastic-tcp-mux-0.2.2.zip
cd meshtastic-tcp-mux
chmod +x install.sh uninstall.sh
sudo ./install.sh
```

If there is no existing install, the installer defaults to a new install. If an
existing install is found at `/opt/meshtastic-tcp-mux`, the installer asks
whether to run an upgrade or a new install.

For an explicit fresh install:

```bash
sudo ./install.sh --mode new
```

For an upgrade that preserves site settings from
`/opt/meshtastic-tcp-mux/meshtastic_tcp_mux.py`:

```bash
sudo ./install.sh --mode upgrade
```

Security defaults `CACHE_REPLAY_TO_NEW_CLIENTS` and
`ALLOW_RAW_WHEN_PROTOBUF_MISSING` deliberately reset to their new safe values
instead of migrating legacy defaults.

During an upgrade, the installer creates a timestamped backup under:

```text
/opt/meshtastic-tcp-mux/backup-YYYYMMDD-HHMMSS
```

The installer copies the source file to:

```text
/opt/meshtastic-tcp-mux/meshtastic_tcp_mux.py
```

It also creates this systemd service:

```text
/etc/systemd/system/meshtastic-tcp-mux.service
```

At the end of installation, the installer runs `--version` and `--check`, then
asks whether to start the service immediately.

## Install from Git

Clone the repository on the Linux machine:

```bash
cd /tmp
git clone https://github.com/Hoser01/meshtastic-tcp-mux.git
cd meshtastic-tcp-mux
chmod +x install.sh uninstall.sh
sudo ./install.sh
```


To install a specific released version from Git:

```bash
cd /tmp
git clone https://github.com/Hoser01/meshtastic-tcp-mux.git
cd meshtastic-tcp-mux
git checkout v0.2.2
chmod +x install.sh uninstall.sh
sudo ./install.sh
```

## Configuration

All configuration is at the top of the Python file:

```bash
sudo nano /opt/meshtastic-tcp-mux/meshtastic_tcp_mux.py
```

Common settings:

```python
REAL_NODE_HOST = "192.168.86.130"
REAL_NODE_PORT = 4403

LISTEN_HOST = "0.0.0.0"
LISTEN_PORT = 4405
```

After changing settings, restart the service:

```bash
sudo systemctl restart meshtastic-tcp-mux
```

If you use a firewall, allow TCP `4405` only from trusted clients.

## Useful Commands

Check current config:

```bash
python3 /opt/meshtastic-tcp-mux/meshtastic_tcp_mux.py --check
```

Check installed version:

```bash
python3 /opt/meshtastic-tcp-mux/meshtastic_tcp_mux.py --version
```

Service status:

```bash
sudo systemctl status meshtastic-tcp-mux --no-pager
```

Follow logs:

```bash
sudo journalctl -u meshtastic-tcp-mux -f
```

Restart service:

```bash
sudo systemctl restart meshtastic-tcp-mux
```

Stop service:

```bash
sudo systemctl stop meshtastic-tcp-mux
```

Disable service:

```bash
sudo systemctl disable --now meshtastic-tcp-mux
```

## Uninstall

From the extracted folder:

```bash
sudo ./uninstall.sh
```

Or manually:

```bash
sudo systemctl disable --now meshtastic-tcp-mux
sudo rm -f /etc/systemd/system/meshtastic-tcp-mux.service
sudo systemctl daemon-reload
sudo rm -rf /opt/meshtastic-tcp-mux
```

## Notes

Default client port is `4405`. This intentionally avoids `4404` so it can be
tested alongside MeshMonitor's virtual node feature.

The mux forwards Meshtastic stream frames. It handles downstream
`ToRadio.disconnect` locally because forwarding a client's session-close
command would close the one shared upstream session. It is not a web server and
does not provide an HTTP interface.

Client scripts should connect to the mux machine on TCP port `4405` instead of
connecting directly to the physical node on `4403`.

## Safety Defaults

`FILTER_CLIENT_ADMIN` is enabled by default. Client-originated decoded packets
on Meshtastic's `ADMIN_APP` port are blocked.

This is meant to reduce the chance of a connected script changing device
settings through the shared proxy.

If protobuf support is unavailable while a filter is enabled, unclassifiable
client frames are blocked. Set `ALLOW_RAW_WHEN_PROTOBUF_MISSING = True` only if
you explicitly accept bypassing those filters.

Cache replay is disabled by default. A rolling cache contains transient radio
events as well as configuration and can make a new consumer process old packets
again. If enabled for a specialized consumer, replay is queued asynchronously
and bounded by both frame count and bytes, so a slow client cannot stall the
radio reader or systemd watchdog.

## Troubleshooting

If the service starts but cannot reach the real node, check:

```bash
ping 192.168.86.130
nc -vz 192.168.86.130 4403
sudo journalctl -u meshtastic-tcp-mux -f
```

If clients cannot connect to the mux, check:

```bash
sudo ss -ltnp | grep 4405
sudo ufw status
```

If using a firewall, allow TCP `4405` from trusted clients only.

## Resilience and Health Checks

The mux supervises its listener loop internally. If the listener loop crashes,
the process logs the full exception, closes client sockets, rebuilds the
selector/listen socket, and restarts the listener. If recovery fails repeatedly,
the process exits with code `1` so systemd can restart it.

The systemd unit installed by `install.sh` enables:

```ini
Type=notify
WatchdogSec=60
Restart=always
```

Status log lines include listener health fields:

```text
listener_alive=True upstream_alive=True listening=True client_count=...
```

Useful recovery checks:

```bash
sudo systemctl status meshtastic-tcp-mux --no-pager
sudo journalctl -u meshtastic-tcp-mux -f
sudo ss -ltnp | grep 4405
```

Losing the upstream node connection should show the upstream state as
`reconnecting`, but it should not kill the listener on port `4405`.
