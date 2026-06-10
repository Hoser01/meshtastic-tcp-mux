#!/usr/bin/env bash
set -euo pipefail

APP_NAME="meshtastic-tcp-mux"
APP_DIR="/opt/${APP_NAME}"
SERVICE_FILE="/etc/systemd/system/${APP_NAME}.service"
SRC_FILE="meshtastic_tcp_mux.py"
VERSION_FILE="VERSION.txt"
MODE=""

usage() {
  cat <<EOF
Usage: sudo ./install.sh [--mode new|upgrade]

  new      Replace any existing install with this release.
  upgrade  Back up the installed app and preserve existing config values.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      MODE="${2:-}"
      shift 2
      ;;
    --mode=*)
      MODE="${1#*=}"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1"
      usage
      exit 1
      ;;
  esac
done

if [[ $EUID -ne 0 ]]; then
  echo "Please run this installer with sudo:"
  echo "  sudo ./install.sh"
  exit 1
fi

if [[ ! -f "${SRC_FILE}" ]]; then
  echo "Could not find ${SRC_FILE}. Run this from the extracted meshtastic-tcp-mux folder."
  exit 1
fi

if [[ -z "${MODE}" ]]; then
  if [[ -f "${APP_DIR}/${SRC_FILE}" ]]; then
    echo "Existing ${APP_NAME} install found at ${APP_DIR}."
    read -r -p "Install mode [upgrade/new] (upgrade): " MODE
    MODE="${MODE:-upgrade}"
  else
    MODE="new"
  fi
fi

if [[ "${MODE}" != "new" && "${MODE}" != "upgrade" ]]; then
  echo "Install mode must be 'new' or 'upgrade'."
  exit 1
fi

BACKUP_DIR=""
if [[ "${MODE}" == "upgrade" && -f "${APP_DIR}/${SRC_FILE}" ]]; then
  BACKUP_DIR="${APP_DIR}/backup-$(date +%Y%m%d-%H%M%S)"
  mkdir -p "${BACKUP_DIR}"
  cp "${APP_DIR}/${SRC_FILE}" "${BACKUP_DIR}/${SRC_FILE}"
  [[ -f "${APP_DIR}/${VERSION_FILE}" ]] && cp "${APP_DIR}/${VERSION_FILE}" "${BACKUP_DIR}/${VERSION_FILE}"
  echo "Backed up existing install to ${BACKUP_DIR}"
elif [[ "${MODE}" == "upgrade" ]]; then
  echo "No existing install found; continuing as a new install."
  MODE="new"
fi

echo "Installing ${APP_NAME} (${MODE})..."

mkdir -p "${APP_DIR}"
cp "${SRC_FILE}" "${APP_DIR}/${SRC_FILE}"
chmod 755 "${APP_DIR}/${SRC_FILE}"
if [[ -f "${VERSION_FILE}" ]]; then
  cp "${VERSION_FILE}" "${APP_DIR}/${VERSION_FILE}"
fi

if [[ "${MODE}" == "upgrade" && -n "${BACKUP_DIR}" ]]; then
  python3 - "${BACKUP_DIR}/${SRC_FILE}" "${APP_DIR}/${SRC_FILE}" <<'PY'
import ast
import re
import sys
from pathlib import Path

old_path = Path(sys.argv[1])
new_path = Path(sys.argv[2])

config_names = {
    "REAL_NODE_HOST",
    "REAL_NODE_PORT",
    "LISTEN_HOST",
    "LISTEN_PORT",
    "MAX_CLIENTS",
    "CLIENT_IDLE_TIMEOUT_SECONDS",
    "CLIENT_RECV_BUFFER",
    "UPSTREAM_RECV_BUFFER",
    "RECONNECT_DELAY_SECONDS",
    "CONNECT_TIMEOUT_SECONDS",
    "SOCKET_KEEPALIVE",
    "OUTBOUND_DELAY_SECONDS",
    "OUTBOUND_QUEUE_SIZE",
    "DROP_CLIENT_IF_QUEUE_FULL",
    "CACHE_REPLAY_TO_NEW_CLIENTS",
    "CACHE_MAX_FRAMES",
    "CACHE_MAX_AGE_SECONDS",
    "FILTER_CLIENT_ADMIN",
    "FILTER_CLIENT_CONFIG",
    "FILTER_CLIENT_MODULE_CONFIG",
    "ALLOW_RAW_WHEN_PROTOBUF_MISSING",
    "LOG_LEVEL",
    "LOG_HEX_FRAMES",
    "LOG_FRAME_SUMMARY",
    "START1",
    "START2",
    "ALT_START2_VALUES",
    "HEADER_LEN",
    "MAX_FRAME_SIZE",
    "HEALTH_CHECK_INTERVAL_SECONDS",
    "LISTENER_RESTART_DELAY_SECONDS",
    "LISTENER_MAX_CONSECUTIVE_FAILURES",
    "SYSTEMD_WATCHDOG_ENABLED",
}

old_tree = ast.parse(old_path.read_text())
values = {}
for node in old_tree.body:
    if isinstance(node, ast.Assign) and len(node.targets) == 1:
        target = node.targets[0]
        if isinstance(target, ast.Name) and target.id in config_names:
            try:
                values[target.id] = repr(ast.literal_eval(node.value))
            except Exception:
                pass

if not values:
    print("No migrated config values found.")
    raise SystemExit(0)

lines = new_path.read_text().splitlines()
changed = []
for idx, line in enumerate(lines):
    match = re.match(r"^([A-Z][A-Z0-9_]*)\s*=", line)
    if match and match.group(1) in values:
        name = match.group(1)
        lines[idx] = f"{name} = {values[name]}"
        changed.append(name)

new_path.write_text("\n".join(lines) + "\n")
print("Migrated config values: " + ", ".join(changed))
PY
fi

cat > "${SERVICE_FILE}" <<EOF
[Unit]
Description=Meshtastic TCP Mux
After=network-online.target
Wants=network-online.target

[Service]
Type=notify
NotifyAccess=main
WorkingDirectory=${APP_DIR}
ExecStart=/usr/bin/python3 ${APP_DIR}/${SRC_FILE}
Restart=always
RestartSec=5
WatchdogSec=60
User=root
Group=root

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable "${APP_NAME}.service"

python3 "${APP_DIR}/${SRC_FILE}" --version
python3 "${APP_DIR}/${SRC_FILE}" --check

echo
read -r -p "Start ${APP_NAME} now? [Y/n] " START_NOW
START_NOW="${START_NOW:-Y}"
if [[ "${START_NOW}" =~ ^[Yy]$ ]]; then
  systemctl restart "${APP_NAME}.service"
  systemctl --no-pager status "${APP_NAME}.service" || true
else
  echo "Not started. Start it later with: sudo systemctl start ${APP_NAME}"
fi

echo
echo "Install complete."
echo
echo "Useful commands:"
echo "  sudo systemctl status ${APP_NAME} --no-pager"
echo "  sudo journalctl -u ${APP_NAME} -f"
echo "  sudo nano ${APP_DIR}/${SRC_FILE}"
echo "  sudo systemctl restart ${APP_NAME}"
echo "  sudo ss -ltnp | grep 4405"
echo
echo "Clients should connect to this machine on TCP port 4405."
