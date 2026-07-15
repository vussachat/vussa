#!/usr/bin/env bash
set -euo pipefail

namespace="${NAMESPACE:-default}"
release="${RELEASE:-vussa}"
deployment="${DEPLOYMENT:-vussa-vussa}"
service="${SERVICE:-$deployment}"
zone="${ZONE:-zone-a}"
nodes=()

cleanup() {
  for node in "${nodes[@]}"; do
    docker start "$node" >/dev/null 2>&1 || true
  done
  for node in "${nodes[@]}"; do
    kubectl wait --for=condition=Ready "node/$node" --timeout=240s >/dev/null 2>&1 || true
  done
}
trap cleanup EXIT

mapfile -t nodes < <(kubectl get nodes -l "topology.kubernetes.io/zone=$zone" \
  -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | sed '/^$/d')
if [[ "${#nodes[@]}" -eq 0 ]]; then
  echo "no Kubernetes nodes found in zone $zone" >&2
  exit 1
fi

for node in "${nodes[@]}"; do
  docker stop "$node" >/dev/null
done
for node in "${nodes[@]}"; do
  kubectl wait --for='condition=Ready=false' "node/$node" --timeout=180s
done

kubectl -n "$namespace" wait --for=condition=available "deployment/$deployment" --timeout=240s
available="$(kubectl -n "$namespace" get deployment "$deployment" -o jsonpath='{.status.availableReplicas}')"
if [[ "${available:-0}" -lt 2 ]]; then
  echo "deployment lost HA capacity after zone $zone failure" >&2
  exit 1
fi

ready_endpoints="$(kubectl -n "$namespace" get endpoints "$service" \
  -o jsonpath='{range .subsets[*].addresses[*]}{.ip}{"\n"}{end}' |
  sed '/^$/d' | sort -u | wc -l | tr -d ' ')"
if [[ "${ready_endpoints:-0}" -lt 2 ]]; then
  echo "service lost ready backend endpoints after zone $zone failure" >&2
  exit 1
fi

kubectl -n "$namespace" run "${release}-zone-failure-probe-$(date +%s)" \
  --rm --restart=Never --attach --quiet \
  --image=curlimages/curl:8.10.1 -- \
  curl --fail --retry 30 --retry-delay 2 "http://$service/api/v1/health/ready"
echo "Kubernetes zone failure and recovery smoke test passed"
