#!/usr/bin/env bash
set -euo pipefail

compose=(docker compose)
cleanup() {
  "${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

"${compose[@]}" up --build --detach
for _ in {1..90}; do
  if curl --fail --silent http://127.0.0.1:3000/api/v1/health/ready >/dev/null; then
    break
  fi
  sleep 2
done
curl --fail --silent http://127.0.0.1:3000/api/v1/health/ready >/dev/null

"${compose[@]}" stop valkey
for _ in {1..30}; do
  if curl --silent http://127.0.0.1:3000/api/v1/health/ready >/dev/null; then
    sleep 1
  else
    break
  fi
done
if curl --silent http://127.0.0.1:3000/api/v1/health/ready >/dev/null; then
  echo "readiness remained healthy while Valkey was stopped" >&2
  exit 1
fi

"${compose[@]}" start valkey
for _ in {1..60}; do
  if curl --fail --silent http://127.0.0.1:3000/api/v1/health/ready >/dev/null; then
    break
  fi
  sleep 2
done
curl --fail --silent http://127.0.0.1:3000/api/v1/health/ready >/dev/null

"${compose[@]}" stop postgres
for _ in {1..30}; do
  if curl --silent http://127.0.0.1:3000/api/v1/health/ready >/dev/null; then
    sleep 1
  else
    break
  fi
done
if curl --silent http://127.0.0.1:3000/api/v1/health/ready >/dev/null; then
  echo "readiness remained healthy while PostgreSQL was stopped" >&2
  exit 1
fi

"${compose[@]}" start postgres
for _ in {1..60}; do
  if curl --fail --silent http://127.0.0.1:3000/api/v1/health/ready >/dev/null; then
    echo "dependency failure recovery smoke test passed"
    exit 0
  fi
  sleep 2
done
echo "readiness did not recover after PostgreSQL restart" >&2
exit 1
