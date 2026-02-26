#!/usr/bin/env bash
set -euo pipefail

APP_MODULE="${GUNICORN_APP_MODULE:-app:app}"
GUNICORN_WORKER_CLASS="${GUNICORN_WORKER_CLASS:-uvicorn.workers.UvicornWorker}"
WEB_CONCURRENCY="${WEB_CONCURRENCY:-1}"
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

  # /etc/secrets is read-only on Render; copy the key to /tmp so we can chmod it
  local tmp_key="/tmp/bastion_key.pem"
  cp "${KEY_PATH}" "${tmp_key}"
  chmod 600 "${tmp_key}"

  echo "[render_start] Starting SSH tunnel localhost:${local_port} -> ${RDS_HOST}:3306 via ${BASTION_HOST}."
  ssh -o StrictHostKeyChecking=no \
    -o ExitOnForwardFailure=yes \
    -o BatchMode=yes \
    -o ConnectTimeout=15 \
    -o ServerAliveInterval=60 \
    -o ServerAliveCountMax=3 \
    -N -L "0.0.0.0:${local_port}:${RDS_HOST}:3306" \
    -i "${tmp_key}" \
    "${BASTION_USER}@${BASTION_HOST}" >/tmp/ssh_tunnel.log 2>&1 &

  # Wait for the tunnel port to be ready before returning (up to 30 seconds)
  local deadline=$(( $(date +%s) + 30 ))
  echo "[render_start] Waiting for tunnel on port ${local_port}..."
  until nc -z 127.0.0.1 "${local_port}" 2>/dev/null; do
    if [[ $(date +%s) -ge ${deadline} ]]; then
      echo "[render_start] WARNING: Tunnel did not become ready within 30s; proceeding anyway."
      echo "[render_start] --- SSH error output ---"
      cat /tmp/ssh_tunnel.log 2>/dev/null || echo "(no ssh log output)"
      echo "[render_start] --- end SSH output ---"
      return
    fi
    sleep 1
  done
  echo "[render_start] Tunnel is ready."
}

if [[ "${APP_TYPE:-fastapi}" == "streamlit" ]]; then
  exec streamlit run app.py --server.port "${PORT}" --server.address 0.0.0.0
fi

start_tunnel

exec gunicorn "${APP_MODULE}" \
  --bind "0.0.0.0:${PORT}" \
  --worker-class "${GUNICORN_WORKER_CLASS}" \
  --workers "${WEB_CONCURRENCY}"
