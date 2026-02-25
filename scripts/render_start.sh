#!/usr/bin/env bash
set -euo pipefail

APP_MODULE="${GUNICORN_APP_MODULE:-app:app}"
KEY_PATH="${BASTION_KEY_PATH:-/etc/secrets/golive-bastion-key.pem}"

start_tunnel() {
  local local_port="${LOCAL_TUNNEL_PORT:-${DB_PORT:-3307}}"

  if [[ -z "${BASTION_HOST:-}" || -z "${BASTION_USER:-}" || -z "${RDS_HOST:-}" ]]; then
    echo "[render_start] Bastion tunnel vars not fully set; skipping SSH tunnel."
    return
  fi

  if [[ ! -f "${KEY_PATH}" ]]; then
    echo "[render_start] Bastion key not found at ${KEY_PATH}; skipping SSH tunnel."
    return
  fi

  chmod 600 "${KEY_PATH}"

  echo "[render_start] Starting SSH tunnel localhost:${local_port} -> ${RDS_HOST}:3306 via ${BASTION_HOST}."
  ssh -o StrictHostKeyChecking=no \
    -o ExitOnForwardFailure=yes \
    -o ServerAliveInterval=60 \
    -o ServerAliveCountMax=3 \
    -N -L "0.0.0.0:${local_port}:${RDS_HOST}:3306" \
    -i "${KEY_PATH}" \
    "${BASTION_USER}@${BASTION_HOST}" &
}

if [[ "${APP_TYPE:-fastapi}" == "streamlit" ]]; then
  exec streamlit run app.py --server.port "${PORT}" --server.address 0.0.0.0
fi

start_tunnel

exec gunicorn "${APP_MODULE}" --bind "0.0.0.0:${PORT}"
