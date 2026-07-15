#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE="$ROOT/.bench"
COMPOSE_FILE="$ROOT/docker-compose.yml"

CONCURRENCY="${1:-${VUSSA_BENCH_CONCURRENCY:-3000}}"
DURATION="${2:-${VUSSA_BENCH_DURATION:-30}}"
MODE="${VUSSA_BENCH_MODE:-mixed}"
MESSAGE_RATE="${VUSSA_BENCH_MESSAGE_RATE:-1}"
API_RATE="${VUSSA_BENCH_API_RATE:-1}"
FULL_API="${VUSSA_BENCH_FULL_API:-true}"
WARMUP="${VUSSA_BENCH_WARMUP:-0}"
SETUP_INTERVAL_MS="${VUSSA_BENCH_SETUP_INTERVAL_MS:-5}"
NOFILE_LIMIT="${VUSSA_NOFILE_LIMIT:-256000}"

POSTGRES_PORT="${VUSSA_POSTGRES_PORT:-5432}"
VALKEY_PORT="${VUSSA_VALKEY_PORT:-6379}"
BACKEND_PORT="${VUSSA_BENCH_BACKEND_PORT:-3000}"
BASE_URL="${VUSSA_BASE_URL:-http://127.0.0.1:${BACKEND_PORT}}"
WS_URL="${VUSSA_WS_URL:-ws://127.0.0.1:${BACKEND_PORT}/api/v1/ws}"
DATABASE_URL="${DATABASE_URL:-postgres://vussa_chat:vussa_chat@127.0.0.1:${POSTGRES_PORT}/vussa_chat}"
VALKEY_URL="${VALKEY_URL:-redis://127.0.0.1:${VALKEY_PORT}}"

PG_MAX_CONNECTIONS="${PG_MAX_CONNECTIONS:-64}"
VALKEY_POOL_SIZE="${VALKEY_POOL_SIZE:-16}"
AUTH_VERIFY_CONCURRENCY="${AUTH_VERIFY_CONCURRENCY:-16}"
WS_ROOM_EVENT_CAPACITY="${WS_ROOM_EVENT_CAPACITY:-4096}"
KEEP_SERVICES="${VUSSA_BENCH_KEEP_SERVICES:-false}"
RUSTFLAGS_VALUE="${VUSSA_BENCH_RUSTFLAGS:--C target-cpu=native -C codegen-units=1 -C opt-level=3 -C panic=abort}"

usage() {
  cat <<'EOF'
usage: ./scripts/bench.sh [concurrency] [duration-seconds]

Defaults to the validated 3,000-user, 30-second mixed WebSocket profile.
The wrapper builds release binaries, starts PostgreSQL/Valkey and the release
backend, waits for readiness, runs the benchmark, and cleans up its processes.

Useful environment overrides:
  VUSSA_BENCH_MODE                  mixed, websocket, api, or readonly
  VUSSA_BENCH_MESSAGE_RATE          messages/user/minute (default: 1)
  VUSSA_BENCH_API_RATE              REST requests/user/minute (default: 1)
  VUSSA_BENCH_FULL_API=false        skip complete API mutation preflight
  WS_ROOM_EVENT_CAPACITY            shared room frame buffer (default: 4096)
  VUSSA_BENCH_SETUP_INTERVAL_MS     per-client ramp interval (default: 5)
  VUSSA_BENCH_SETUP_TIMEOUT         global authentication/WS timeout
  VUSSA_BENCH_OUTPUT                JSON report path
  VUSSA_BENCH_KEEP_SERVICES=true    leave PostgreSQL and Valkey running
  VUSSA_NOFILE_LIMIT                process file limit (default: 256000)
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi
if (( $# > 2 )); then
  usage >&2
  exit 2
fi

require_positive_integer() {
  local name="$1"
  local value="$2"
  if [[ ! "$value" =~ ^[0-9]+$ ]] || (( value < 1 )); then
    echo "$name must be one positive integer (got: $value)" >&2
    exit 2
  fi
}

require_nonnegative_integer() {
  local name="$1"
  local value="$2"
  if [[ ! "$value" =~ ^[0-9]+$ ]]; then
    echo "$name must be a non-negative integer (got: $value)" >&2
    exit 2
  fi
}

require_positive_integer "concurrency" "$CONCURRENCY"
require_nonnegative_integer "duration" "$DURATION"
require_positive_integer "VUSSA_BENCH_SETUP_INTERVAL_MS" "$SETUP_INTERVAL_MS"
require_positive_integer "VUSSA_NOFILE_LIMIT" "$NOFILE_LIMIT"

# Authentication and WebSocket setup are separately ramped. Preserve the
# validated 5 ms pacing for large runs and grow the global deadline as needed.
minimum_setup_timeout=$((
  (2 * CONCURRENCY * SETUP_INTERVAL_MS + 999) / 1000 + 30
))
if [[ -n "${VUSSA_BENCH_SETUP_TIMEOUT:-}" ]]; then
  SETUP_TIMEOUT="$VUSSA_BENCH_SETUP_TIMEOUT"
  require_positive_integer "VUSSA_BENCH_SETUP_TIMEOUT" "$SETUP_TIMEOUT"
else
  SETUP_TIMEOUT=90
  if (( minimum_setup_timeout > SETUP_TIMEOUT )); then
    SETUP_TIMEOUT="$minimum_setup_timeout"
  fi
fi

mkdir -p "$STATE"
OUTPUT="${VUSSA_BENCH_OUTPUT:-$STATE/latest.json}"
BACKEND_LOG="$STATE/backend.log"
BUILD_LOG="$STATE/build.log"
mkdir -p "$(dirname "$OUTPUT")"

BACKEND_PID=""
POSTGRES_WAS_RUNNING=false
VALKEY_WAS_RUNNING=false

service_was_running() {
  local service="$1"
  grep -qx "$service" <<< "$RUNNING_SERVICES"
}

stop_backend() {
  if [[ -n "$BACKEND_PID" ]] && kill -0 "$BACKEND_PID" 2>/dev/null; then
    kill "$BACKEND_PID" 2>/dev/null || true
    # Reap the child immediately so Bash does not print a signal-status job
    # notification during otherwise successful benchmark cleanup.
    wait "$BACKEND_PID" 2>/dev/null || true
  fi
}

cleanup() {
  local status=$?
  trap - EXIT INT TERM
  stop_backend

  if [[ "$KEEP_SERVICES" != "true" ]]; then
    if [[ "$POSTGRES_WAS_RUNNING" == "false" && "$VALKEY_WAS_RUNNING" == "false" ]]; then
      VUSSA_POSTGRES_PORT="$POSTGRES_PORT" VUSSA_VALKEY_PORT="$VALKEY_PORT" \
        docker compose -f "$COMPOSE_FILE" down >/dev/null 2>&1 || true
    else
      if [[ "$POSTGRES_WAS_RUNNING" == "false" ]]; then
        VUSSA_POSTGRES_PORT="$POSTGRES_PORT" VUSSA_VALKEY_PORT="$VALKEY_PORT" \
          docker compose -f "$COMPOSE_FILE" stop postgres >/dev/null 2>&1 || true
      fi
      if [[ "$VALKEY_WAS_RUNNING" == "false" ]]; then
        VUSSA_POSTGRES_PORT="$POSTGRES_PORT" VUSSA_VALKEY_PORT="$VALKEY_PORT" \
          docker compose -f "$COMPOSE_FILE" stop valkey >/dev/null 2>&1 || true
      fi
    fi
  fi
  exit "$status"
}
trap cleanup EXIT INT TERM

if ! ulimit -n "$NOFILE_LIMIT" 2>/dev/null; then
  echo "Could not raise the open-file limit to $NOFILE_LIMIT." >&2
  echo "Run './scripts/dev.sh setup-limits' first or lower VUSSA_NOFILE_LIMIT." >&2
  exit 1
fi
echo "Open-file limit: soft=$(ulimit -Sn) hard=$(ulimit -Hn)"

if curl -fsS --max-time 1 "$BASE_URL/api/v1/health/startup" >/dev/null 2>&1; then
  echo "A backend is already listening at $BASE_URL; stop it before benchmarking." >&2
  exit 1
fi

RUNNING_SERVICES="$(
  VUSSA_POSTGRES_PORT="$POSTGRES_PORT" VUSSA_VALKEY_PORT="$VALKEY_PORT" \
    docker compose -f "$COMPOSE_FILE" ps --status running --services 2>/dev/null || true
)"
if service_was_running postgres; then POSTGRES_WAS_RUNNING=true; fi
if service_was_running valkey; then VALKEY_WAS_RUNNING=true; fi

echo "Starting PostgreSQL and Valkey..."
VUSSA_POSTGRES_PORT="$POSTGRES_PORT" VUSSA_VALKEY_PORT="$VALKEY_PORT" \
  docker compose -f "$COMPOSE_FILE" up -d --wait postgres valkey

echo "Building optimized release server and benchmark..."
(
  cd "$ROOT"
  CARGO_INCREMENTAL=0 RUSTFLAGS="$RUSTFLAGS_VALUE" \
    cargo build --release -p vussa -p vussa-bench
) >"$BUILD_LOG" 2>&1 || {
  echo "Release build failed; see $BUILD_LOG" >&2
  tail -40 "$BUILD_LOG" >&2 || true
  exit 1
}

# Cargo normally creates executable mode bits for binaries, but an existing
# target artifact can retain a non-executable mode on macOS. Repair both
# artifacts explicitly so the benchmark never fails before the server starts.
chmod u+x "$ROOT/target/release/vussa" "$ROOT/target/release/vussa-bench"
if [[ ! -x "$ROOT/target/release/vussa" || ! -x "$ROOT/target/release/vussa-bench" ]]; then
  echo "Release artifacts are not executable: $ROOT/target/release" >&2
  ls -l "$ROOT/target/release/vussa" "$ROOT/target/release/vussa-bench" >&2 || true
  exit 1
fi

: > "$BACKEND_LOG"
echo "Starting release backend..."
env \
  DATABASE_URL="$DATABASE_URL" \
  VALKEY_URL="$VALKEY_URL" \
  BIND_ADDRESS="127.0.0.1:${BACKEND_PORT}" \
  SEED_TEST_ACCOUNTS=true \
  AUTH_VERIFY_CONCURRENCY="$AUTH_VERIFY_CONCURRENCY" \
  PG_MAX_CONNECTIONS="$PG_MAX_CONNECTIONS" \
  VALKEY_POOL_SIZE="$VALKEY_POOL_SIZE" \
  WS_ROOM_EVENT_CAPACITY="$WS_ROOM_EVENT_CAPACITY" \
  "$ROOT/target/release/vussa" >"$BACKEND_LOG" 2>&1 &
BACKEND_PID=$!

ready=false
for _ in {1..200}; do
  if curl -fsS --max-time 1 "$BASE_URL/api/v1/health/startup" >/dev/null 2>&1; then
    ready=true
    break
  fi
  if ! kill -0 "$BACKEND_PID" 2>/dev/null; then
    break
  fi
  sleep 0.1
done
if [[ "$ready" != "true" ]]; then
  echo "Release backend failed to become ready; see $BACKEND_LOG" >&2
  tail -40 "$BACKEND_LOG" >&2 || true
  exit 1
fi

FULL_API_ARGS=()
if [[ "$FULL_API" == "true" ]]; then
  FULL_API_ARGS=(--full-api --allow-mutations)
fi

echo "Running: users=$CONCURRENCY duration=${DURATION}s setup_timeout=${SETUP_TIMEOUT}s mode=$MODE"
echo "Traffic: messages=${MESSAGE_RATE}/user/min API=${API_RATE}/user/min full_api=$FULL_API"
(
  cd "$ROOT"
  target/release/vussa-bench \
    --base-url "$BASE_URL" \
    --ws-url "$WS_URL" \
    --mode "$MODE" \
    --concurrency "$CONCURRENCY" \
    --duration "$DURATION" \
    --warmup "$WARMUP" \
    --message-rate "$MESSAGE_RATE" \
    --api-rate "$API_RATE" \
    --setup-interval-ms "$SETUP_INTERVAL_MS" \
    --setup-timeout "$SETUP_TIMEOUT" \
    --nofile-limit "$NOFILE_LIMIT" \
    --output "$OUTPUT" \
    "${FULL_API_ARGS[@]}"
)

echo "Report: $OUTPUT"
echo "Backend log: $BACKEND_LOG"
