#!/usr/bin/env bash
# Provisions dbxcarta Neo4j credentials into a Databricks secret scope.
#
# Usage:
#   ./setup_secrets.sh [--profile NAME] [ENV_FILE]
#
# ENV_FILE defaults to .env at the dbxcarta repo root. The script reads:
#   DATABRICKS_SECRET_SCOPE  optional, defaults to dbxcarta-neo4j
#   DATABRICKS_PROFILE       optional, used when --profile is not provided
#   NEO4J_URI                required
#   NEO4J_USERNAME           required
#   NEO4J_PASSWORD           required
#
# Secret key names are uppercase by design. They match the keys read by the
# Databricks jobs and databricks-job-runner secret injection.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${ROOT_DIR}/.env"
PROFILE="${DATABRICKS_CONFIG_PROFILE:-${DATABRICKS_PROFILE:-}}"

usage() {
  sed -n '2,15p' "$0" | sed 's/^# \{0,1\}//'
}

trim() {
  local value="$1"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "$value"
}

env_value() {
  local key="$1"
  local line raw_key raw_value value

  while IFS= read -r line || [[ -n "$line" ]]; do
    line="$(trim "$line")"
    [[ -z "$line" || "$line" == \#* ]] && continue
    [[ "$line" == export\ * ]] && line="$(trim "${line#export }")"
    [[ "$line" != *=* ]] && continue

    raw_key="$(trim "${line%%=*}")"
    [[ "$raw_key" != "$key" ]] && continue

    raw_value="$(trim "${line#*=}")"
    if [[ "$raw_value" == \"*\" && "$raw_value" == *\" ]]; then
      value="${raw_value:1:${#raw_value}-2}"
    elif [[ "$raw_value" == \'*\' && "$raw_value" == *\' ]]; then
      value="${raw_value:1:${#raw_value}-2}"
    else
      value="$raw_value"
    fi
    printf '%s' "$value"
    return 0
  done < "$ENV_FILE"

  return 1
}

required_env() {
  local key="$1"
  local value
  value="$(env_value "$key" || true)"
  if [[ -z "$value" || "$value" == *\<* || "$value" == *\>* ]]; then
    echo "Error: $key is not set in $ENV_FILE." >&2
    exit 1
  fi
  printf '%s' "$value"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -p|--profile)
      if [[ $# -lt 2 ]]; then
        echo "Error: --profile requires a value." >&2
        exit 1
      fi
      PROFILE="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      ENV_FILE="$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
      shift
      ;;
  esac
done

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Error: $ENV_FILE not found." >&2
  echo "Copy .env.sample to .env at the dbxcarta repo root and fill in values." >&2
  exit 1
fi

if ! command -v databricks >/dev/null 2>&1; then
  echo "Error: databricks CLI not found." >&2
  exit 1
fi

PROFILE="${PROFILE:-$(env_value DATABRICKS_PROFILE || true)}"
if [[ -z "$PROFILE" || "$PROFILE" == *\<* || "$PROFILE" == *\>* ]]; then
  echo "Available Databricks profiles:"
  databricks auth profiles 2>/dev/null || echo "  (could not list profiles; check ~/.databrickscfg)"
  echo
  read -r -p "Profile name [DEFAULT]: " PROFILE
  PROFILE="${PROFILE:-DEFAULT}"
fi

export DATABRICKS_CONFIG_PROFILE="$PROFILE"
echo "Using Databricks profile: $DATABRICKS_CONFIG_PROFILE"

SCOPE="$(env_value DATABRICKS_SECRET_SCOPE || true)"
SCOPE="${SCOPE:-dbxcarta-neo4j}"
if [[ "$SCOPE" == *\<* || "$SCOPE" == *\>* ]]; then
  echo "Error: DATABRICKS_SECRET_SCOPE is still a placeholder in $ENV_FILE." >&2
  exit 1
fi

NEO4J_URI="$(required_env NEO4J_URI)"
NEO4J_USERNAME="$(required_env NEO4J_USERNAME)"
NEO4J_PASSWORD="$(required_env NEO4J_PASSWORD)"

ensure_scope() {
  local scope="$1"
  local output rc

  set +e
  output="$(databricks secrets create-scope "$scope" 2>&1)"
  rc=$?
  set -e

  if [[ "$rc" -eq 0 ]]; then
    echo "Created secret scope: $scope"
  elif [[ "$output" == *"already exists"* ]]; then
    echo "Secret scope already exists: $scope"
  else
    echo "Error creating scope $scope: $output" >&2
    exit 1
  fi
}

put_secret() {
  local scope="$1"
  local key="$2"
  local value="$3"
  printf '  - %s/%s\n' "$scope" "$key"
  databricks secrets put-secret "$scope" "$key" --string-value "$value"
}

echo
echo "Writing dbxcarta Neo4j secrets"
ensure_scope "$SCOPE"
put_secret "$SCOPE" "NEO4J_URI" "$NEO4J_URI"
put_secret "$SCOPE" "NEO4J_USERNAME" "$NEO4J_USERNAME"
put_secret "$SCOPE" "NEO4J_PASSWORD" "$NEO4J_PASSWORD"

echo
echo "Done. Jobs read these with dbutils.secrets.get(scope, key)."
