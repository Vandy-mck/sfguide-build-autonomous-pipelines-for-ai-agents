#!/usr/bin/env bash
#
# Usage: ./helpers/setup_rpk_profile.sh [profile-name]
#
# Creates a local rpk profile using KAFKA_* credentials from .env.
# Configures SASL/SCRAM-SHA-256 over TLS for Redpanda Cloud.
#
# Prerequisites:
#   1. Copy .env.template to .env and fill in KAFKA_BOOTSTRAP_SERVERS,
#      KAFKA_USERNAME, and KAFKA_PASSWORD.
#   2. Ensure 'rpk' CLI is installed.
#
# Examples:
#   ./helpers/setup_rpk_profile.sh              # uses KAFKA_USERNAME as profile name
#   ./helpers/setup_rpk_profile.sh my-profile   # custom profile name
#
set -euo pipefail

ENV_FILE=".env"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Error: .env file not found. Run 'cp .env.template .env' and fill in your values."
  exit 1
fi

source "$ENV_FILE"

PROFILE_NAME="${1:-${KAFKA_USERNAME}}"

echo "Setting up rpk profile: ${PROFILE_NAME}"
echo "  Broker: ${KAFKA_BOOTSTRAP_SERVERS}"
echo "  User:   ${KAFKA_USERNAME}"

rpk profile create "${PROFILE_NAME}" 2>/dev/null || rpk profile use "${PROFILE_NAME}"

rpk profile set kafka_api.tls.enabled="true"
rpk profile set brokers="${KAFKA_BOOTSTRAP_SERVERS}"
rpk profile set sasl.mechanism="SCRAM-SHA-256"
rpk profile set user="${KAFKA_USERNAME}"
rpk profile set pass="${KAFKA_PASSWORD}"

echo ""
echo "Profile '${PROFILE_NAME}' configured. Testing connection..."
rpk cluster info 2>&1 && echo "OK — connected." || echo "FAILED — check credentials/network."
