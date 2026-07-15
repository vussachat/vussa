#!/usr/bin/env bash
set -euo pipefail

namespace="${NAMESPACE:-default}"
postgres_label="${POSTGRES_LABEL:-app=postgres}"
pod="$(kubectl -n "$namespace" get pods -l "$postgres_label" -o jsonpath='{.items[0].metadata.name}')"
test -n "$pod"
archive=/tmp/vussa-ha-smoke.dump

cleanup() {
  kubectl -n "$namespace" exec "$pod" -- dropdb -U vussa_chat --if-exists vussa_restore >/dev/null 2>&1 || true
  kubectl -n "$namespace" exec "$pod" -- rm -f "$archive" >/dev/null 2>&1 || true
}
trap cleanup EXIT

kubectl -n "$namespace" exec "$pod" -- sh -ec \
  "pg_dump --format=custom --no-owner -U vussa_chat -d vussa_chat > '$archive'"
kubectl -n "$namespace" exec "$pod" -- pg_restore --list "$archive" >/dev/null
kubectl -n "$namespace" exec "$pod" -- createdb -U vussa_chat vussa_restore
kubectl -n "$namespace" exec "$pod" -- pg_restore --no-owner --exit-on-error \
  -U vussa_chat -d vussa_restore "$archive"
tables="$(kubectl -n "$namespace" exec "$pod" -- psql -U vussa_chat -d vussa_restore -Atc \
  "SELECT count(*) FROM pg_catalog.pg_tables WHERE schemaname='public'")"
if [[ "${tables:-0}" -lt 1 ]]; then
  echo "restored database has no public tables" >&2
  exit 1
fi
echo "Kubernetes PostgreSQL backup and restore smoke test passed"
