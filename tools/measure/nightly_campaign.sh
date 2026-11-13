#!/usr/bin/env bash
# Nightly measurement campaign, started by cron every evening until END_DATE (inclusive).
# Runs the whole campaign (run_campaign.sh) on the framework code as it is in the repository that evening, into
# ~/thesis/measurements/thesis-final/<date>/ (log of all nights: thesis-final/nightly.log). The image of the night is built if it is missing and pinned, so commits
# made while the campaign runs do not change what is measured.
#
# crontab (the hour is in the time zone of the machine):
#   0 20 * * * /bin/bash $HOME/thesis/latency-sensitive-faas/tools/measure/nightly_campaign.sh
# Manual start of tonight's campaign (resumes it if it was interrupted): bash tools/measure/nightly_campaign.sh

END_DATE=${END_DATE:-2026-12-31}
REPO=${REPO:-$HOME/thesis/latency-sensitive-faas}
NIGHTLY=${NIGHTLY:-$HOME/thesis/measurements/thesis-final}
export PATH=/usr/local/bin:/usr/local/go/bin:/usr/bin:/bin:$HOME/.local/bin:$PATH   # cron starts with a minimal PATH

today=$(date +%F)
mkdir -p "$NIGHTLY"
log() { echo "$(date -u +%FT%TZ) $*" >> "$NIGHTLY/nightly.log"; }

if [[ "$today" > "$END_DATE" ]]; then
  log "after $END_DATE, nothing to do (remove the crontab line)"
  exit 0
fi

# One campaign at a time: a campaign still running from the night before keeps the machine
exec 9> "$NIGHTLY/.lock"
if ! flock -n 9; then
  log "SKIP $today: another nightly campaign is still running"
  exit 0
fi

# A manual measurement in progress is not disturbed: wait for it up to an hour
for _ in $(seq 60); do
  pgrep -f tools/measure/loadgen.py > /dev/null || break
  sleep 60
done
if pgrep -f tools/measure/loadgen.py > /dev/null; then
  log "SKIP $today: a manual measurement is still running"
  exit 0
fi

cd "$REPO" || { log "SKIP $today: repository not found at $REPO"; exit 1; }
export ROOT=$NIGHTLY/$today
mkdir -p "$ROOT"
export DEV_IMAGE=lsf-configurator:dev-$(git log -1 --format=%h -- lsf-configurator)

# What is measured tonight: the commit and any uncommitted change of the framework code
{
  echo "image:  $DEV_IMAGE"
  echo "commit: $(git log -1 --format='%h %s')"
  echo "branch: $(git rev-parse --abbrev-ref HEAD)"
  echo "uncommitted changes:"
  git status --short -- lsf-configurator tools/measure
} > "$ROOT/code.txt"

if ! minikube -p knative image ls 2>/dev/null | grep -q "${DEV_IMAGE}\$"; then
  log "building $DEV_IMAGE"
  if ! { docker build -q -t "$DEV_IMAGE" -f lsf-configurator/Dockerfile lsf-configurator \
         && minikube -p knative image load "$DEV_IMAGE"; } >> "$ROOT/build.log" 2>&1; then
    log "SKIP $today: building $DEV_IMAGE failed (see $ROOT/build.log)"
    exit 1
  fi
fi

log "START $today with $DEV_IMAGE"
bash tools/measure/run_campaign.sh >> "$ROOT/campaign.out" 2>&1
log "END $today: $(find "$ROOT" -name DONE | wc -l) runs done"