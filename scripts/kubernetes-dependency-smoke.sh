#!/usr/bin/env bash
set -euo pipefail

namespace="${NAMESPACE:-default}"
probe_image="${PROBE_IMAGE:-curlimages/curl:8.10.1}"
valkey_deployment="${VALKEY_DEPLOYMENT:-valkey}"
app_service="${SERVICE:-vussa-vussa}"
probe_name="vussa-dependency-probe-$(date +%s)"

kubectl -n "$namespace" scale "deployment/$valkey_deployment" --replicas=0
cleanup_probe() {
  kubectl -n "$namespace" scale "deployment/$valkey_deployment" --replicas=1 >/dev/null 2>&1 || true
  kubectl -n "$namespace" delete pod "$probe_name" --ignore-not-found >/dev/null 2>&1 || true
}
trap cleanup_probe EXIT
for _ in {1..60}; do
  if [[ -z "$(kubectl -n "$namespace" get pods -l app=valkey -o name)" ]]; then break; fi
  sleep 2
done
if [[ -n "$(kubectl -n "$namespace" get pods -l app=valkey -o name)" ]]; then
  echo "Valkey pod did not terminate" >&2
  exit 1
fi

if kubectl -n "$namespace" run "$probe_name" --rm --restart=Never --attach --quiet \
  --image="$probe_image" -- curl --fail --max-time 5 "http://$app_service/api/v1/health/ready"; then
  echo "readiness unexpectedly succeeded while Valkey was unavailable" >&2
  exit 1
fi

kubectl -n "$namespace" scale "deployment/$valkey_deployment" --replicas=1
kubectl -n "$namespace" wait --for=condition=available "deployment/$valkey_deployment" --timeout=180s
kubectl -n "$namespace" run "$probe_name-recovered" --rm --restart=Never --attach --quiet \
  --image="$probe_image" -- curl --fail --retry 30 --retry-delay 2 "http://$app_service/api/v1/health/ready"
echo "Kubernetes dependency outage and recovery smoke test passed"
