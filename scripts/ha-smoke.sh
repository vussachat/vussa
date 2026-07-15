#!/usr/bin/env bash
set -euo pipefail

# Run against an already-installed release. The script intentionally uses only
# Kubernetes APIs plus a local port-forward. It verifies replica placement,
# authenticated application traffic, readiness, and rolling replacement.
namespace="${NAMESPACE:-default}"
release="${RELEASE:-vussa}"
deployment="${DEPLOYMENT:-$release-vussa}"
service="${SERVICE:-$deployment}"
backend_label="${BACKEND_LABEL:-vussa}"
backend_selector="app.kubernetes.io/name=$backend_label,app.kubernetes.io/instance=$release"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
port_forward_pid=""

assert_backend_topology() {
  local nodes zones
  nodes="$(kubectl -n "$namespace" get pods -l "$backend_selector" -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}' | sed '/^$/d' | sort -u | wc -l | tr -d ' ' )"
  if [[ "${nodes:-0}" -lt 2 ]]; then
    echo "backend replicas are not distributed across at least two nodes" >&2
    exit 1
  fi
  zones="$(while read -r node; do
    [[ -z "$node" ]] || kubectl get node "$node" -o jsonpath='{.metadata.labels.topology\.kubernetes\.io/zone}'
    printf '\n'
  done < <(kubectl -n "$namespace" get pods -l "$backend_selector" -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}' | sed '/^$/d' | sort -u | sed '/^$/d') | sed '/^$/d' | sort -u | wc -l | tr -d ' ' )"
  if [[ "${zones:-0}" -lt 2 ]]; then
    echo "backend replicas are not distributed across at least two zones" >&2
    exit 1
  fi
}

cleanup() {
  [[ -z "$port_forward_pid" ]] || kill "$port_forward_pid" 2>/dev/null || true
}
trap cleanup EXIT

kubectl -n "$namespace" rollout status "deployment/$deployment" --timeout=120s
kubectl -n "$namespace" wait --for=condition=available "deployment/$deployment" --timeout=120s

pdb_name="$deployment"
kubectl -n "$namespace" get pdb "$pdb_name" >/dev/null
pdb_allowed="$(kubectl -n "$namespace" get pdb "$pdb_name" -o jsonpath='{.status.disruptionsAllowed}')"
if [[ "${pdb_allowed:-0}" -lt 1 ]]; then
  echo "pod disruption budget does not permit a safe voluntary disruption" >&2
  exit 1
fi

ready_endpoints="$(kubectl -n "$namespace" get endpoints "$service" -o jsonpath='{range .subsets[*].addresses[*]}{.ip}{"\n"}{end}' | sed '/^$/d' | sort -u | wc -l | tr -d ' ' )"
if [[ "${ready_endpoints:-0}" -lt 2 ]]; then
  echo "service has fewer than two ready backend endpoints" >&2
  exit 1
fi

replicas="$(kubectl -n "$namespace" get deployment "$deployment" -o jsonpath='{.status.availableReplicas}')"
if [[ "${replicas:-0}" -lt 2 ]]; then
  echo "expected at least two available replicas, got ${replicas:-0}" >&2
  exit 1
fi

assert_backend_topology

kubectl -n "$namespace" get pods -l "$backend_selector" -o wide
kubectl -n "$namespace" run "${release}-ha-probe-$(date +%s)" \
  --rm --restart=Never --attach --quiet \
  --image=curlimages/curl:8.10.1 -- \
  curl --fail --retry 10 --retry-delay 2 "http://$service/api/v1/health/ready"

local_port="${HA_LOCAL_PORT:-3300}"
kubectl -n "$namespace" port-forward "service/$service" "$local_port:3000" >/tmp/"$release"-port-forward.log 2>&1 &
port_forward_pid=$!
for _ in {1..30}; do
  if curl --fail --silent "http://127.0.0.1:$local_port/api/v1/health/live" >/dev/null; then
    break
  fi
  sleep 1
done
(
  cd "$repo_root"
  START_SERVER=false BASE_URL="http://127.0.0.1:$local_port" scripts/integration-smoke.sh
)

victim="$(kubectl -n "$namespace" get pods -l "$backend_selector" -o jsonpath='{.items[0].metadata.name}')"
test -n "$victim"
kubectl -n "$namespace" delete pod "$victim" --wait=false
for _ in {1..60}; do
  ready="$(kubectl -n "$namespace" get pods -l "$backend_selector" -o jsonpath='{range .items[*]}{.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}' | awk '$1 == "True" {count++} END {print count+0}')"
  if [[ "${ready:-0}" -ge 2 ]]; then break; fi
  sleep 2
done
if [[ "${ready:-0}" -lt 2 ]]; then
  echo "replacement pod did not become ready" >&2
  exit 1
fi
assert_backend_topology
ready_endpoints="$(kubectl -n "$namespace" get endpoints "$service" -o jsonpath='{range .subsets[*].addresses[*]}{.ip}{"\n"}{end}' | sed '/^$/d' | sort -u | wc -l | tr -d ' ' )"
if [[ "${ready_endpoints:-0}" -lt 2 ]]; then
  echo "service did not recover two ready backend endpoints" >&2
  exit 1
fi
kill "$port_forward_pid" 2>/dev/null || true
wait "$port_forward_pid" 2>/dev/null || true
kubectl -n "$namespace" port-forward "service/$service" "$local_port:3000" >/tmp/"$release"-port-forward.log 2>&1 &
port_forward_pid=$!
for _ in {1..30}; do
  if curl --fail --silent "http://127.0.0.1:$local_port/api/v1/health/live" >/dev/null; then
    break
  fi
  sleep 1
done
kubectl -n "$namespace" run "${release}-failover-probe-$(date +%s)" \
  --rm --restart=Never --attach --quiet \
  --image=curlimages/curl:8.10.1 -- \
  curl --fail --retry 20 --retry-delay 2 "http://$service/api/v1/health/ready"

(
  cd "$repo_root"
  START_SERVER=false BASE_URL="http://127.0.0.1:$local_port" scripts/integration-smoke.sh
)

kubectl -n "$namespace" rollout restart "deployment/$deployment"
kubectl -n "$namespace" rollout status "deployment/$deployment" --timeout=180s
kubectl -n "$namespace" wait --for=condition=available "deployment/$deployment" --timeout=120s
echo "HA smoke test passed"
