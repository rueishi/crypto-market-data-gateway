#!/usr/bin/env bash
#
# Runs the gateway against Coinbase Exchange Direct Feed L2 using the sample
# YAML config. Invoke from the repository root:
#
#   ./scripts/run-coinbase-l2-direct.sh
#
# Required environment variables:
#   COINBASE_EXCHANGE_API_KEY
#   COINBASE_EXCHANGE_API_SECRET
#   COINBASE_EXCHANGE_API_PASSPHRASE
#
# Optional:
#   --config <path>  Override the YAML config path.
#
# The script builds target/classes and a Maven dependency classpath, then starts
# GatewayBootstrap with -Dgateway.config. It leaves the process running until
# Ctrl+C/SIGTERM, at which point the gateway shutdown hook should drain.

set -euo pipefail

config_path="config/coinbase-l2-direct.yaml"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config|-c)
      if [[ $# -lt 2 ]]; then
        echo "Missing value for $1" >&2
        exit 1
      fi
      config_path="$2"
      shift 2
      ;;
    --help|-h)
      echo "Usage: $0 [--config <path>]"
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      echo "Usage: $0 [--config <path>]" >&2
      exit 1
      ;;
  esac
done

require_env() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    echo "Missing required environment variable: $name" >&2
    exit 1
  fi
}

require_env "COINBASE_EXCHANGE_API_KEY"
require_env "COINBASE_EXCHANGE_API_SECRET"
require_env "COINBASE_EXCHANGE_API_PASSPHRASE"

if [[ ! -f "$config_path" ]]; then
  echo "Config file not found: $config_path" >&2
  exit 1
fi

mkdir -p target var/run/coinbase-l2-direct

mvn -q -DskipTests package dependency:build-classpath -Dmdep.outputFile=target/runtime-classpath.txt

dependency_classpath="$(tr -d '\r\n' < target/runtime-classpath.txt)"
if [[ -z "$dependency_classpath" ]]; then
  echo "Maven did not write target/runtime-classpath.txt" >&2
  exit 1
fi

runtime_classpath="target/classes:${dependency_classpath}"

exec java "-Dgateway.config=${config_path}" \
  -cp "$runtime_classpath" \
  io.rueishi.marketdata.crypto.core.bootstrap.GatewayBootstrap
