#!/usr/bin/env bash
set -euo pipefail

namespace="${NAMESPACE:-default}"
release="${RELEASE:-vussa}"
backend_label="${BACKEND_LABEL:-vussa}"
deployment="${DEPLOYMENT:-vussa-vussa}"
service="${SERVICE:-$deployment}"
node="${NODE:-}"
node_stopped=false

cleanup() {
  if [[ "$node_stopped" == true ]]; then
    docker start "$node" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if [[ -z "$node" ]]; then
  node="$(kubectl -n "$namespace" get pods -l "app.kubernetes.io/name=$backend_label,app.kubernetes.io/instance=$release" \
    -o jsonpath='{.items[0].spec.nodeName}')"
fi
test -n "$node"
docker stop "$node" >/dev/null
node_stopped=true
kubectl wait --for='condition=Ready=false' "node/$node" --timeout=180s
kubectl -n "$namespace" wait --for=condition=available "deployment/$deployment" --timeout=240s

ready="$(kubectl -n "$namespace" get deployment "$deployment" -o jsonpath='{.status.availableReplicas}')"
if [[ "${ready:-0}" -lt 2 ]]; then
  echo "deployment did not reschedule two available replicas after node failure" >&2
  exit 1
fi

kubectl -n "$namespace" run "node-failure-probe-$(date +%s)" \
  --rm --restart=Never --attach --quiet \
  --image=curlimages/curl:8.10.1 -- \
  curl --fail --retry 30 --retry-delay 2 "http://$service/api/v1/health/ready"

docker start "$node" >/dev/null
node_stopped=false
kubectl wait --for=condition=Ready "node/$node" --timeout=240s
echo "Kubernetes node failure and recovery smoke test passed"
