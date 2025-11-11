#!/usr/bin/env sh
set -eu

# Defaults (can be overridden via env)
CONFIG_PATH=${CONFIG_PATH:-/app/config.json}
GLOBAL_CONFIG_PATH=${GLOBAL_CONFIG_PATH:-/app/global-config.json}
KEYS_PATH=${KEYS_PATH:-/app/keys.json}
GLOBAL_CONFIG_URL=${GLOBAL_CONFIG_URL:-https://testnet.tychoprotocol.com/global-config.json}
DATA_DIR=${DATA_DIR:-/app/data}

mkdir -p "${DATA_DIR}"

# Initialize default config if not present
if [ ! -f "${CONFIG_PATH}" ]; then
  echo "[entrypoint] Generating default config at ${CONFIG_PATH}"
  tycho-tonapi run --init-config "${CONFIG_PATH}"
fi

# Download global config if not present
if [ ! -f "${GLOBAL_CONFIG_PATH}" ]; then
  echo "[entrypoint] Downloading global config from ${GLOBAL_CONFIG_URL} to ${GLOBAL_CONFIG_PATH}"
  if command -v wget >/dev/null 2>&1; then
    wget -O "${GLOBAL_CONFIG_PATH}" "${GLOBAL_CONFIG_URL}"
  elif command -v curl >/dev/null 2>&1; then
    curl -fsSL -o "${GLOBAL_CONFIG_PATH}" "${GLOBAL_CONFIG_URL}"
  else
    echo "Neither wget nor curl is available in the container." >&2
    exit 1
  fi
fi

# Build run arguments from env, allow overriding via container args
RUN_ARGS="run --config ${CONFIG_PATH} --global-config ${GLOBAL_CONFIG_PATH}"

# keys file is optional; include if exists

RUN_ARGS="${RUN_ARGS} --keys ${KEYS_PATH}"


# Append any extra args passed to container
if [ "$#" -gt 0 ]; then
  RUN_ARGS="${RUN_ARGS} $*"
fi

echo "[entrypoint] Executing: tycho-tonapi ${RUN_ARGS}"
exec tycho-tonapi ${RUN_ARGS}
