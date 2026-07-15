#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE="$ROOT/.dev"
mkdir -p "$STATE"
SEED_MARKER="$STATE/test-accounts-seeded"
POSTGRES_PORT="${VUSSA_POSTGRES_PORT:-5432}"
VALKEY_PORT="${VUSSA_VALKEY_PORT:-6379}"
DATABASE_URL="${DATABASE_URL:-postgres://vussa_chat:vussa_chat@127.0.0.1:${POSTGRES_PORT}/vussa_chat}"
VALKEY_URL="${VALKEY_URL:-redis://127.0.0.1:${VALKEY_PORT}}"
# Development fixtures are always enabled when this wrapper owns the backend.
export SEED_TEST_ACCOUNTS=true
NOFILE_LIMIT="${VUSSA_NOFILE_LIMIT:-256000}"

current_soft_nofile() {
  ulimit -Sn
}

current_hard_nofile() {
  ulimit -Hn
}

validate_nofile_limit() {
  if [[ ! "$NOFILE_LIMIT" =~ ^[0-9]+$ ]] || (( NOFILE_LIMIT < 1024 )); then
    echo "VUSSA_NOFILE_LIMIT must be an integer >= 1024 (got: $NOFILE_LIMIT)" >&2
    return 1
  fi
}

raise_nofile_limit() {
  validate_nofile_limit
  local before_soft before_hard after_soft
  before_soft="$(current_soft_nofile)"
  before_hard="$(current_hard_nofile)"
  if ! ulimit -n "$NOFILE_LIMIT" 2>/dev/null; then
    echo "Cannot raise the open-file limit to $NOFILE_LIMIT (soft=$before_soft hard=$before_hard)." >&2
    echo "Run '$0 setup-limits' first, or set VUSSA_NOFILE_LIMIT to an allowed value." >&2
    return 1
  fi
  after_soft="$(current_soft_nofile)"
  if (( after_soft < NOFILE_LIMIT )); then
    echo "Open-file limit is $after_soft; required $NOFILE_LIMIT (hard=$before_hard)." >&2
    return 1
  fi
  echo "Open-file limit: soft=$after_soft hard=$(current_hard_nofile)"
}

setup_limits() {
  validate_nofile_limit
  if [[ "$(uname -s)" != "Darwin" ]]; then
    echo "Persistent launchd limits are only supported by this command on macOS." >&2
    return 1
  fi
  echo "Setting launchd maxfiles to $NOFILE_LIMIT (requires sudo)..."
  sudo launchctl limit maxfiles "$NOFILE_LIMIT" "$NOFILE_LIMIT"
  local launchd_limits
  launchd_limits="$(launchctl limit maxfiles)"
  echo "$launchd_limits"
  if ! awk -v required="$NOFILE_LIMIT" 'NR == 1 { soft_ok = ($2 == "unlimited" || $2 + 0 >= required); hard_ok = ($3 == "unlimited" || $3 + 0 >= required); exit !(soft_ok && hard_ok) }' <<< "$launchd_limits"; then
    echo "launchd did not report maxfiles >= $NOFILE_LIMIT" >&2
    return 1
  fi
  echo "Persistent launchd file-descriptor limit configured."
}

check_limits() {
  validate_nofile_limit
  echo "Shell:   soft=$(current_soft_nofile) hard=$(current_hard_nofile)"
  if [[ "$(uname -s)" == "Darwin" ]]; then
    echo "launchd: $(launchctl limit maxfiles)"
  fi
  if [[ -f "$STATE/backend.pid" ]]; then
    local pid
    pid="$(cat "$STATE/backend.pid")"
    if kill -0 "$pid" 2>/dev/null; then
      echo "Backend: pid=$pid (inherited limits from its dev.sh parent)"
    else
      echo "Backend: pid=$pid is not running"
    fi
  else
    echo "Backend: not started"
  fi
}

process_matches() {
  local file="$1"
  local pattern="$2"
  [[ -f "$file" ]] || return 1
  local pid command
  pid="$(cat "$file")"
  kill -0 "$pid" 2>/dev/null || return 1
  command="$(ps -p "$pid" -o command= 2>/dev/null || true)"
  case "$command" in
    *"$pattern"*) return 0 ;;
    *) return 1 ;;
  esac
}

stop_process() {
  local file="$1"
  local pattern="$2"
  local port="${3:-}"
  local pid=""
  if [[ -f "$file" ]]; then
    pid="$(cat "$file")"
  elif [[ -n "$port" ]]; then
    pid="$(lsof -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null | head -1 || true)"
  fi
  if [[ -n "$pid" ]] && { [[ ! -f "$file" ]] || process_matches "$file" "$pattern"; }; then
    pkill -TERM -P "$pid" 2>/dev/null || true
    kill "$pid" 2>/dev/null || true
    for _ in {1..20}; do
      kill -0 "$pid" 2>/dev/null || break
      sleep 0.1
    done
    kill -9 "$pid" 2>/dev/null || true
  fi
  rm -f "$file"
}

stop_orphaned_listener() {
  local port="$1"
  local pattern="$2"
  local pid command
  while read -r pid; do
    [[ -n "$pid" ]] || continue
    command="$(ps -p "$pid" -o command= 2>/dev/null || true)"
    case "$command" in
      *"$pattern"*)
        pkill -TERM -P "$pid" 2>/dev/null || true
        kill "$pid" 2>/dev/null || true
        ;;
    esac
  done < <(lsof -tiTCP:"$port" -sTCP:LISTEN 2>/dev/null || true)
}

wait_for_url() {
  local name="$1"
  local url="$2"
  for _ in {1..30}; do
    if curl -fsS --max-time 1 "$url" >/dev/null 2>&1; then
      echo "$name ready: $url"
      return 0
    fi
    sleep 1
  done
  echo "$name failed to become ready: $url" >&2
  return 1
}

start() {
  raise_nofile_limit
  VUSSA_POSTGRES_PORT="$POSTGRES_PORT" VUSSA_VALKEY_PORT="$VALKEY_PORT" \
    docker compose -f "$ROOT/docker-compose.yml" up -d --wait valkey postgres
  if ! process_matches "$STATE/backend.pid" "target/debug/vussa" || [[ ! -f "$SEED_MARKER" ]]; then
    stop_process "$STATE/backend.pid" "target/debug/vussa"
    rm -f "$STATE/backend.pid"
    (cd "$ROOT" && cargo build -p vussa) >"$STATE/backend-build.log" 2>&1
    (
      cd "$ROOT"
      printf 'Backend launch limits: soft=%s hard=%s\n' "$(ulimit -Sn)" "$(ulimit -Hn)" >"$STATE/backend.log"
      nohup env DATABASE_URL="$DATABASE_URL" VALKEY_URL="$VALKEY_URL" SEED_TEST_ACCOUNTS="$SEED_TEST_ACCOUNTS" target/debug/vussa \
        >>"$STATE/backend.log" 2>&1 < /dev/null &
      echo $! > "$STATE/backend.pid"
    )
  fi
  if ! process_matches "$STATE/frontend.pid" "node_modules/vite"; then
    rm -f "$STATE/frontend.pid"
    if [[ ! -x "$ROOT/frontend/node_modules/.bin/vite" ]]; then
      (cd "$ROOT/frontend" && npm install) >"$STATE/frontend-install.log" 2>&1
    fi
    (
      cd "$ROOT/frontend"
      nohup "$ROOT/frontend/node_modules/.bin/vite" dev --host 0.0.0.0 \
        >"$STATE/frontend.log" 2>&1 < /dev/null &
      echo $! > "$STATE/frontend.pid"
    )
  fi
  if ! wait_for_url "Backend" "http://127.0.0.1:3000/api/v1/health/startup"; then
    echo "See $STATE/backend.log and $STATE/backend-build.log" >&2
    return 1
  fi
  touch "$SEED_MARKER"
  if ! wait_for_url "Frontend" "http://127.0.0.1:5173/"; then
    echo "See $STATE/frontend.log and $STATE/frontend-install.log" >&2
    return 1
  fi
  echo "Backend: http://localhost:3000"
  echo "Frontend: http://localhost:5173"
}

stop() {
  stop_process "$STATE/backend.pid" "target/debug/vussa" 3000
  stop_process "$STATE/frontend.pid" "node_modules/vite" 5173
  stop_orphaned_listener 3000 "target/debug/vussa"
  stop_orphaned_listener 5173 "node_modules/vite"
  rm -f "$SEED_MARKER"
  VUSSA_POSTGRES_PORT="$POSTGRES_PORT" VUSSA_VALKEY_PORT="$VALKEY_PORT" \
    docker compose -f "$ROOT/docker-compose.yml" down --remove-orphans
}

clean() {
  stop_process "$STATE/backend.pid" "target/debug/vussa" 3000
  stop_process "$STATE/frontend.pid" "node_modules/vite" 5173
  stop_orphaned_listener 3000 "target/debug/vussa"
  stop_orphaned_listener 5173 "node_modules/vite"
  rm -f "$SEED_MARKER"
  VUSSA_POSTGRES_PORT="$POSTGRES_PORT" VUSSA_VALKEY_PORT="$VALKEY_PORT" \
    docker compose -f "$ROOT/docker-compose.yml" down --volumes --remove-orphans
  rm -f "$STATE/backend.log" "$STATE/frontend.log"
  echo "Vussa development containers, network, and volumes removed; Docker images were kept."
}

case "${1:-}" in
  start) start ;;
  stop) stop ;;
  restart) stop; start ;;
  setup-limits) setup_limits ;;
  check-limits) check_limits ;;
  clean)
    if [[ "${2:-}" == "restart" ]]; then clean; start; else clean; fi
    ;;
  clean-restart) clean; start ;;
  *) echo "usage: $0 {start|stop|restart|setup-limits|check-limits|clean [restart]|clean-restart}" >&2; exit 1 ;;
esac
