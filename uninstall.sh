#!/usr/bin/env bash
set -euo pipefail

APP_NAME="meshtastic-tcp-mux"
APP_DIR="/opt/${APP_NAME}"
SERVICE_FILE="/etc/systemd/system/${APP_NAME}.service"

if [[ $EUID -ne 0 ]]; then
  echo "Please run with sudo: sudo ./uninstall.sh"
  exit 1
fi

systemctl stop "${APP_NAME}.service" 2>/dev/null || true
systemctl disable "${APP_NAME}.service" 2>/dev/null || true
rm -f "${SERVICE_FILE}"
systemctl daemon-reload

echo "Service removed."
read -r -p "Remove ${APP_DIR} too? [y/N] " REMOVE_DIR
REMOVE_DIR="${REMOVE_DIR:-N}"
if [[ "${REMOVE_DIR}" =~ ^[Yy]$ ]]; then
  rm -rf "${APP_DIR}"
  echo "Removed ${APP_DIR}."
else
  echo "Left ${APP_DIR} in place."
fi
