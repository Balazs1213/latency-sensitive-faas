#!/usr/bin/env bash
# Final measurement campaign: every configuration on every load profile, repeated, unattended.
#
# Usage (inside tmux, from anywhere):
#   bash tools/measure/run_campaign.sh              all runs (REPS=3 by default), resumable
#   DRY=1 bash tools/measure/run_campaign.sh        only the environment checks, no measurement
#   ONLY="steps:follow-forecast:1" bash ...         one run (profile:config:rep), e.g. as a pilot
#
# Output: ~/thesis/measurements/thesis-final/<profile>/<config>/rep<N>/ ; a run with a DONE file is skipped,
# so the campaign can simply be started again after an interruption.
# Order: within a repetition every configuration once (both profiles after one config switch), and the
# configuration order is rotated in every repetition, so drifts of the machine do not hit only the last ones.

source ~/thesis/measure-env.sh   # measure venv, repository directory

ROOT=${ROOT:-~/thesis/measurements/thesis-final}
REPS=${REPS:-3}
LIMIT=1000
APP=4dr6u38ufksqzmew9f3twvriw
HOST=app-$APP.application.127.0.0.1.sslip.io
NS=application
CONFIGS=(baseline full proactive follow follow-forecast)
PROFILES=(ramp steps)
SETTLE=${SETTLE:-60}   # extra quiet time before a run, after the app is on the small layout

declare -A PROFILE PHASES
PROFILE[ramp]="30:2,120:2-14,90:14"
PHASES[ramp]="idle:0-30,ramp up:30-150,peak:150-240"
PROFILE[steps]="60:1,60:3,60:6,60:9,60:12,120:14,60:9,60:5,360:1,180:10,360:1"
PHASES[steps]="warm-up:0-60,staircase up:60-420,staircase down:420-540,quiet 1:540-900,sudden jump:900-1080,quiet 2:1080-1440"

mkdir -p "$ROOT"
LOG=$ROOT/campaign.log
log() { echo "$(date -u +%FT%TZ) $*" | tee -a "$LOG"; }

# --- environment checks -------------------------------------------------------------------------

es_ok() { curl -sS -m 5 -u elastic:elastic localhost:9200/_cluster/health >/dev/null 2>&1; }

ensure_es() {  # restarts the Elasticsearch port-forward if it died
  es_ok && return 0
  local svc
  svc=$(kubectl get svc -n observability -o name | grep -m1 'es-http')
  log "Elasticsearch not reachable, starting a port-forward to $svc"
  pkill -f "port-forward.*9200" 2>/dev/null
  nohup kubectl port-forward -n observability "$svc" 9200:9200 >> "$ROOT/es-port-forward.log" 2>&1 &
  for _ in $(seq 12); do sleep 5; es_ok && return 0; done
  return 1
}

disk_ok() {  # the second vmstat line: blocks read in and I/O wait
  local bi wa
  read -r bi wa < <(vmstat 5 2 | tail -1 | awk '{print $9, $16}')
  if [ "${bi:-0}" -gt 5000 ] || [ "${wa:-0}" -gt 6 ]; then
    log "disk is busy: bi=$bi wa=$wa"; return 1
  fi
}

ksvcs() { kubectl get ksvc -n $NS -o name | sed 's#.*/##' | grep '^d-'; }

rev_ok() {  # the latest created revision of the Knative service is Ready
  local rev
  rev=$(kubectl get ksvc "$1" -n $NS -o jsonpath='{.status.latestCreatedRevisionName}')
  [ -n "$rev" ] && [ "$(kubectl get revision "$rev" -n $NS \
    -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}')" = "True" ]
}

repair_revisions() {  # forces a new revision where the latest one is not Ready (e.g. ExitCode0 after overload)
  local s ok=0
  for s in $(ksvcs); do
    rev_ok "$s" && continue
    log "revision of $s is not ready, forcing a new one"
    kubectl patch ksvc "$s" -n $NS --type merge -p \
      "{\"spec\":{\"template\":{\"metadata\":{\"annotations\":{\"lsf.thesis/restarted-at\":\"$(date -u +%FT%TZ)\"}}}}}" >/dev/null
    for _ in $(seq 24); do rev_ok "$s" && break; sleep 5; done
    rev_ok "$s" || ok=1
  done
  return $ok
}

pods_ok() {  # all d- pods Running and fully ready
  kubectl get pods -n $NS --no-headers 2>/dev/null | grep '^d-' | grep -v Terminating | \
    awk '{n++; split($2,a,"/"); if ($3!="Running" || a[1]!=a[2]) bad=1} END{exit (bad || n==0)}'
}

active_layout() {  # active_layout_key of the app, from the configurator API through the API server proxy
  kubectl get --raw "/api/v1/namespaces/configurator/services/lsf-configurator:80/proxy/function_apps/$APP" \
    2>/dev/null | python -c 'import sys, json; print(json.load(sys.stdin).get("active_layout_key", ""))' 2>/dev/null
}

wait_small_layout() {  # without traffic the controller switches back to min_rate by itself
  local layout
  for _ in $(seq 72); do
    layout=$(active_layout)
    [ "$layout" = "min_rate" ] && return 0
    sleep 5
  done
  log "app is still on '${layout:-unknown}' after 6 min"
  return 1
}

reset_baseline_min_scale() {
  # The prewarm modes set min-scale on the PodAutoscalers directly; the baseline image does not reset it.
  # Restore every PodAutoscaler to the min-scale of its Knative service template (Egon's value, default 1).
  local s rev ms
  for s in $(ksvcs); do
    rev=$(kubectl get ksvc "$s" -n $NS -o jsonpath='{.status.latestReadyRevisionName}')
    ms=$(kubectl get ksvc "$s" -n $NS -o jsonpath='{.spec.template.metadata.annotations.autoscaling\.knative\.dev/min-scale}')
    [ -n "$rev" ] || continue
    kubectl patch podautoscalers.autoscaling.internal.knative.dev "$rev" -n $NS --type merge \
      -p "{\"metadata\":{\"annotations\":{\"autoscaling.knative.dev/min-scale\":\"${ms:-1}\"}}}" >/dev/null \
      && log "baseline: $s min-scale -> ${ms:-1}"
  done
}

check_env() {  # 0 when the environment is clean enough for a measurement
  local attempt
  for attempt in 1 2 3; do
    local bad=0
    ensure_es || { log "Elasticsearch still not reachable"; bad=1; }
    repair_revisions || bad=1
    for _ in $(seq 36); do pods_ok && break; sleep 5; done
    pods_ok || { log "pods are not ready"; bad=1; }
    wait_small_layout || bad=1
    disk_ok || { bad=1; sleep 60; }
    [ $bad = 0 ] && return 0
    log "environment check $attempt failed"
  done
  return 1
}

# --- configuration and one run ------------------------------------------------------------------

apply_config() {  # $1 config, $2 directory for config.txt
  case $1 in
    follow-forecast) RATE_FORECAST=true tools/measure/use_config.sh follow "$2" ;;
    *)               RATE_FORECAST=false tools/measure/use_config.sh "$1" "$2" ;;
  esac
  [ "$1" = baseline ] && reset_baseline_min_scale
  return 0
}

measure() {  # $1 profile name, $2 run directory, $3 run id
  local D=$2 START END PW
  echo "${PROFILE[$1]}" > "$D/profile.txt"
  kubectl get pods -n $NS -o wide | grep d- | grep -v Terminating > "$D/pods-before.txt"
  python tools/measure/podwatch.py --out "$D" & PW=$!
  START=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  python tools/measure/loadgen.py --host "$HOST" --profile "${PROFILE[$1]}" --cameras 8 --run-id "$3" --out "$D"
  sleep 30
  END=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  kill $PW 2>/dev/null
  kubectl logs -n configurator deploy/lsf-configurator-deployment --since-time="$START" > "$D/controller.log"
  ensure_es
  python tools/measure/collect_traces.py --app $APP --start "$START" --end "$END" --out "$D"
  python tools/measure/reconfig_breakdown.py --run "$D" --limit $LIMIT
  python tools/measure/plot_run.py --run "$D" --limit $LIMIT
  python tools/measure/plot_phases.py --run "$D" --limit $LIMIT --phases "${PHASES[$1]}" \
    --title "$1 / $(basename "$(dirname "$D")") / $(basename "$D")"
  [ -s "$D/traces.csv" ] && [ -s "$D/phases.csv" ]
}

FAILS=0
run_one() {  # $1 profile, $2 config, $3 rep; assumes the config is applied
  local D=$ROOT/$1/$2/rep$3
  [ -f "$D/DONE" ] && { log "skip $1/$2/rep$3 (done)"; return; }
  if ! check_env; then
    FAILS=$((FAILS+1)); log "SKIP $1/$2/rep$3: environment not clean ($FAILS in a row)"
    [ $FAILS -ge 3 ] && { log "aborting after 3 failed checks"; exit 1; }
    return
  fi
  FAILS=0
  rm -rf "$D"; mkdir -p "$D"
  cp "$ROOT/.config-$2.txt" "$D/config.txt" 2>/dev/null
  log "START $1/$2/rep$3"
  sleep "$SETTLE"
  if measure "$1" "$D" "final-$1-$2-r$3-$(date +%H%M)" > "$D/run_output.txt" 2>&1; then
    date -u +%FT%TZ > "$D/DONE"; log "END $1/$2/rep$3"
  else
    log "FAILED $1/$2/rep$3 (see run_output.txt), will be repeated on the next start"
  fi
}

needs_config() {  # true if any run of this config in this repetition is not done yet
  local p
  for p in "${PROFILES[@]}"; do [ -f "$ROOT/$p/$1/rep$2/DONE" ] || return 0; done
  return 1
}

# --- main ---------------------------------------------------------------------------------------

if [ -n "$DRY" ]; then
  log "DRY: active layout = '$(active_layout)', d- services: $(ksvcs | tr '\n' ' ')"
  check_env && log "DRY: environment is clean" || log "DRY: environment is NOT clean"
  exit 0
fi

if [ -n "$ONLY" ]; then
  IFS=: read -r p c r <<< "$ONLY"
  log "ONLY $p/$c/rep$r"
  apply_config "$c" "$ROOT/.config-$c.txt.d" > "$ROOT/.config-$c.log" 2>&1
  cp "$ROOT/.config-$c.txt.d/config.txt" "$ROOT/.config-$c.txt" 2>/dev/null
  run_one "$p" "$c" "$r"
  exit 0
fi

log "campaign start: $REPS repetitions, configs ${CONFIGS[*]}, profiles ${PROFILES[*]}"
n=${#CONFIGS[@]}
for rep in $(seq "$REPS"); do
  for k in $(seq 0 $((n-1))); do
    cfg=${CONFIGS[$(( (k + rep - 1) % n ))]}
    needs_config "$cfg" "$rep" || { log "skip $cfg/rep$rep (done)"; continue; }
    log "config $cfg (rep $rep)"
    apply_config "$cfg" "$ROOT/.config-$cfg.txt.d" > "$ROOT/.config-$cfg.log" 2>&1
    cp "$ROOT/.config-$cfg.txt.d/config.txt" "$ROOT/.config-$cfg.txt" 2>/dev/null
    for prof in "${PROFILES[@]}"; do
      run_one "$prof" "$cfg" "$rep"
    done
  done
done
python tools/measure/summarize.py "$ROOT" >> "$LOG" 2>&1
log "campaign finished"