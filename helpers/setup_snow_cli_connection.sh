#!/usr/bin/env bash
#
# Usage: ./helpers/setup_snow_cli.sh
#
# Reads credentials from .env and configures a Snowflake CLI connection
# using a Programmatic Access Token (PAT).
#
# Prerequisites:
#   1. Copy .env.template to .env and fill in your values.
#   2. Ensure 'snow' CLI is installed and on your PATH.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE=".env"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Error: .env file not found. Run 'cp .env.template .env' and fill in your values."
  exit 1
fi

source "$ENV_FILE"

TOKEN_FILE="${HOME}/.snowflake/pat_token_${SNOWFLAKE_CONNECTION_NAME}"
mkdir -p "${HOME}/.snowflake"
echo -n "${SNOWFLAKE_PAT}" > "${TOKEN_FILE}"
chmod 600 "${TOKEN_FILE}"

snow connection remove "${SNOWFLAKE_CONNECTION_NAME}" 2>/dev/null || true

snow connection add \
  --connection-name "${SNOWFLAKE_CONNECTION_NAME}" \
  --account "${SNOWFLAKE_ACCOUNT}" \
  --user "${SNOWFLAKE_USER}" \
  --role "${SNOWFLAKE_ROLE}" \
  --warehouse "${SNOWFLAKE_WAREHOUSE}" \
  --database "${DCM_DATABASE}" \
  --schema "${DCM_SCHEMA}" \
  --authenticator "PROGRAMMATIC_ACCESS_TOKEN" \
  --token-file-path "${TOKEN_FILE}" \
  --default

echo "Connection '${SNOWFLAKE_CONNECTION_NAME}' created. Testing..."
snow connection test --connection "${SNOWFLAKE_CONNECTION_NAME}"
