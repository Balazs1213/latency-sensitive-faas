#!/usr/bin/env bash
# Switch the configurator to one measured configuration (image + prewarm mode) and record it.
# Usage: tools/measure/use_config.sh <baseline|off|one|full|adaptive|proactive> [run_dir]
set -euo pipefail

NS=configurator
DEPLOY=deploy/lsf-configurator-deployment
CONTAINER=lsf-configurator
BASELINE_IMAGE=${BASELINE_IMAGE:-lsf-configurator:baseline}
# The dev image is tagged with the last commit that changed the framework code (run inside the repository)
DEV_IMAGE=${DEV_IMAGE:-lsf-configurator:dev-$(git log -1 --format=%h -- lsf-configurator)}

config=${1:?"usage: use_config.sh <baseline|off|one|full|adaptive|proactive> [run_dir]"}
run_dir=${2:-}

# Set the image, the prewarm mode and the proactive switch factor (always all three, so no
# setting is left over from the previous configuration), then wait until the new configurator pod runs
apply() {
  kubectl set image -n "$NS" "$DEPLOY" "$CONTAINER=$1" >/dev/null
  kubectl set env -n "$NS" "$DEPLOY" PREWARM_MODE="$2" SWITCH_RATE_FACTOR="$3" >/dev/null
  kubectl rollout status -n "$NS" "$DEPLOY" --timeout=400s
}

case "$config" in
  baseline)
    # The baseline image cannot remove standby deployments itself: clean up in off mode first
    apply "$DEV_IMAGE" off 0
    echo "Waiting 60 s for the off-mode cleanup..."
    sleep 60
    apply "$BASELINE_IMAGE" off 0
    ;;
  off|one|full|adaptive)
    apply "$DEV_IMAGE" "$config" 0
    ;;
  proactive)
    # Warm standby (full) plus switching on the arrival rate
    apply "$DEV_IMAGE" full 1.0
    ;;
  *)
    echo "unknown config: $config" >&2
    exit 1
    ;;
esac

echo "Waiting 60 s for the standby layouts to settle..."
sleep 60

# Record what is running now
summary() {
  echo "config: $config"
  echo "time:   $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "image:  $(kubectl get -n "$NS" "$DEPLOY" -o jsonpath='{.spec.template.spec.containers[0].image}')"
  echo "digest: $(kubectl get pods -n "$NS" -l app=lsf-configurator --sort-by=.metadata.creationTimestamp \
    -o jsonpath='{.items[-1:].status.containerStatuses[0].imageID}')"
  echo "env:"
  kubectl set env -n "$NS" "$DEPLOY" --list | grep -E "PREWARM|BOOST|SWITCH|TARGET_|MCPU|METRIC|TICK" | sed 's/^/  /'
  echo "knative services:"
  kubectl get ksvc -n application --no-headers | awk '{print "  " $1 "  ready=" $5}'
  echo "pods:"
  kubectl get pods -n application --no-headers | grep -v Terminating | awk '{print "  " $1 "  " $3}'
}

if [ -n "$run_dir" ]; then
  mkdir -p "$run_dir"
  summary | tee "$run_dir/config.txt"
else
  summary
fi