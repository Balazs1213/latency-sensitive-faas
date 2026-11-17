#!/usr/bin/env bash
# Measurement campaign of one day: the configurations in normal operation, then the fault and overload
# experiments, all repeated, unattended. Started every evening by nightly_campaign.sh; it can also be run by hand.
#
# Usage (from anywhere):
#   bash tools/measure/run_campaign.sh                      everything, resumable (a run with a DONE file is skipped)
#   DRY=1 bash tools/measure/run_campaign.sh                only the environment checks, no measurement
#   ONLY=2-faults/1-pod-loss/with-spare-pods/2-fusion-only-pod bash ...    one experiment (all its repetitions)
#   ONLY=1-normal-operation/ramp-4min/1-baseline-original bash ...
#
# Output (the same every day): $ROOT, by default ~/thesis/measurements/thesis-final/<date>/
#   README.txt  what each folder contains            code.txt     commit and image (written by nightly_campaign.sh)
#   summary.txt one line per run                     campaign.log progress
#   1-normal-operation/<profile>/<N-configuration>/repN/
#   2-faults/1-pod-loss/<without|with>-spare-pods/<N-experiment>/repN/            in the follow-forecast configuration;
#   2-faults/2-node-unavailable/<without|with>-backup-layout/<N-experiment>/repN/  with-: one protection switched on
#   3-overload/<N-experiment>/repN/
#   configs/<configuration>/           config.txt and use_config.sh output of the last switch to it
# Every run folder: phases.txt, phases.csv, plots/, run.png, config.txt, traces.csv, loadgen.csv, controller.log;
# the fault and overload runs also fault.txt (when and what was injected) and fault_timeline.txt (request outcomes).

source ~/thesis/measure-env.sh   # measure venv, repository directory

ROOT=${ROOT:-~/thesis/measurements/thesis-final/$(date +%F)}
REPS=${REPS:-2}
LIMIT=1000
APP=4dr6u38ufksqzmew9f3twvriw
HOST=app-$APP.application.127.0.0.1.sslip.io
NS=application
SETTLE=${SETTLE:-60}   # extra quiet time before a run, after the app is on the small layout

# --- what is measured ---------------------------------------------------------------------------

# Configurations: folder name -> use_config.sh call (see apply_config)
CONFIGS=(1-baseline-original 2-full-prewarm 3-proactive-early-switch 4-follow-rate 5-follow-rate-forecast)
FAULT_CONFIG=5-follow-rate-forecast

# Normal-operation profiles: folder name -> loadgen profile and phases
declare -A PROFILE PHASES
PROFILE[ramp-4min]="30:2,120:2-14,90:14"
PHASES[ramp-4min]="idle:0-30,ramp up:30-150,peak:150-240"
PROFILE[steps-24min]="60:1,60:3,60:6,60:9,60:12,120:14,60:9,60:5,360:1,180:10,360:1"
PHASES[steps-24min]="warm-up:0-60,staircase up:60-420,staircase down:420-540,quiet 1:540-900,sudden jump:900-1080,quiet 2:1080-1440"
PROFILES=(ramp-4min steps-24min)

# Fault and overload experiments, all in the final configuration (follow-forecast). In 2-faults/ every fault has a
# without-/with- pair that differs in one protection only:
#   1-pod-loss/with-spare-pods/            SPARE_PODS=single: a spare pod for the compositions planned for one pod
#   2-node-unavailable/with-backup-layout/  BACKUP_LAYOUT=cold: the compositions move away from an unavailable node
# 3-overload/ has no protection yet. The command runs in the repository; deployment ids and node names are looked
# up when the fault is injected.
FAULT_PROFILE="30:2,60:2-14,210:14"
FAULT_PHASES="idle:0-30,ramp up:30-90,before the fault:90-180,fault:180-270,after the fault:270-300"
NODE_PROFILE="30:2,60:2-14,330:14"   # long enough to see the node coming back
FUSION=position-service,ru-fusion-service
DETECT=detect-track-service
FUSION_NODE="N=\$(python tools/measure/deployment_id.py --components $FUSION --field node)"
SPARE_CONFIG=5-follow-rate-forecast-spare-pods
BACKUP_CONFIG=5-follow-rate-forecast-backup-cold
declare -A E_CONFIG E_PROFILE E_PHASES E_LOADGEN E_DELAY E_CMD E_TEXT
EXPERIMENTS=()   # in run order, grouped by configuration, so the configuration switches only twice

# experiment <folder> <configuration> <profile> <phases> <loadgen options> <fault delay s> <fault command> <text>
experiment() {
  EXPERIMENTS+=("$1")
  E_CONFIG[$1]=$2; E_PROFILE[$1]=$3; E_PHASES[$1]=$4; E_LOADGEN[$1]=$5; E_DELAY[$1]=$6; E_CMD[$1]=$7; E_TEXT[$1]=$8
}

delete_one_pod() {  # prints the command that deletes one pod of the deployment of the given components
  echo "kubectl delete -n $NS --wait=false \$(kubectl get pods -n $NS -o name -l serving.knative.dev/service=\$(python tools/measure/deployment_id.py --components $1) | head -1)"
}

pod_loss() {  # $1 without-spare-pods or with-spare-pods, $2 configuration
  experiment 2-faults/1-pod-loss/$1/1-detect-pod-of-many "$2" "$FAULT_PROFILE" "$FAULT_PHASES" "" 180 \
    "$(delete_one_pod $DETECT)" \
    "H1a: one of the several detect pods is deleted at 180 s"
  experiment 2-faults/1-pod-loss/$1/2-fusion-only-pod "$2" "$FAULT_PROFILE" "$FAULT_PHASES" "" 180 \
    "$(delete_one_pod $FUSION)" \
    "H1b: one position+fusion pod is deleted at 180 s: the only one, or one of two with a spare pod"
}

node_unavailable() {  # $1 without-backup-layout or with-backup-layout, $2 configuration
  experiment 2-faults/2-node-unavailable/$1/1-node-loss-90s "$2" "$NODE_PROFILE" \
    "idle:0-30,ramp up:30-90,before the fault:90-180,node lost:180-270,node back:270-420" "--workers 1024" 180 \
    "$FUSION_NODE; kubectl cordon \$N && kubectl delete pod -n $NS -l serving.knative.dev/service=\$(python tools/measure/deployment_id.py --components $FUSION) --wait=false; sleep 90; kubectl uncordon \$N" \
    "H2: the node of the position+fusion pods is lost for 90 s from 180 s (emulated: cordon and pod deletion, then uncordon)"
  experiment 2-faults/2-node-unavailable/$1/2-node-overload-simulated-90s "$2" "$NODE_PROFILE" \
    "idle:0-30,ramp up:30-90,before the fault:90-180,node marked:180-270,mark removed:270-420" "--workers 1024" 180 \
    "$FUSION_NODE; kubectl annotate node \$N lsf-configurator/overloaded=true --overwrite; sleep 90; kubectl annotate node \$N lsf-configurator/overloaded-" \
    "H3: the node of the position+fusion pods is marked overloaded for 90 s from 180 s (simulated: the node is not loaded)"
}

pod_loss without-spare-pods "$FAULT_CONFIG"
node_unavailable without-backup-layout "$FAULT_CONFIG"
experiment 3-overload/1-cpu-noisy-neighbour-pod "$FAULT_CONFIG" "$FAULT_PROFILE" "$FAULT_PHASES" "" 180 \
  "kubectl delete -f $HOME/thesis/cpu-stress.yaml --ignore-not-found --wait=true; kubectl apply -f $HOME/thesis/cpu-stress.yaml" \
  "T2a: a BestEffort pod with 16 busy threads runs for 90 s from 180 s on the node of the detect pods"
experiment 3-overload/2-cpu-whole-host-90s "$FAULT_CONFIG" "$FAULT_PROFILE" "$FAULT_PHASES" "" 180 \
  "$HOME/thesis/host-cpu-stress.sh 24 90" \
  "T2b: 24 busy threads on the whole machine (all nodes share it) for 90 s from 180 s"
experiment 3-overload/3-requests-30rps-open-loop "$FAULT_CONFIG" "30:2,60:2-14,60:14,30:14-30,120:30" \
  "idle:0-30,ramp up:30-90,14 per s:90-150,ramp to 30:150-180,30 per s:180-300" "--workers 1024" "" "" \
  "T1b: the request rate rises to 30/s, above the capacity; 1024 sender threads, so the sender does not slow down"
pod_loss with-spare-pods "$SPARE_CONFIG"
node_unavailable with-backup-layout "$BACKUP_CONFIG"

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

uncordon_nodes() {  # a node left unschedulable by an interrupted node-loss experiment
  local n
  for n in $(kubectl get nodes -o jsonpath='{range .items[?(@.spec.unschedulable==true)]}{.metadata.name}{" "}{end}'); do
    log "node $n was left unschedulable, uncordoning it"
    kubectl uncordon "$n" >/dev/null
  done
}

ksvcs() { kubectl get ksvc -n $NS -o name | sed 's#.*/##' | grep '^d-'; }

app_json() { kubectl get --raw "/api/v1/namespaces/configurator/services/lsf-configurator:80/proxy/function_apps/$APP" 2>/dev/null; }

known_deployments() {  # deployment ids the controller's database knows (any composition, any layout)
  app_json | python -c '
import sys, json
a = json.load(sys.stdin)
print("\n".join(d["id"] for fc in a.get("compositions") or [] for d in fc.get("deployments") or []))'
}

ORPHAN_AGE_S=${ORPHAN_AGE_S:-900}   # a service younger than this may still be in creation (stored after its deploy)
remove_orphans() {  # d- services the controller no longer knows: left over from an earlier configuration
  local known s created age
  known=$(known_deployments) || return 0
  [ -n "$known" ] || return 0   # API not reachable or empty answer: never delete blindly
  for s in $(ksvcs); do
    grep -qx "$s" <<< "$known" && continue
    created=$(kubectl get ksvc "$s" -n $NS -o jsonpath='{.metadata.creationTimestamp}')
    age=$(( $(date +%s) - $(date -d "$created" +%s) ))
    [ "$age" -lt "$ORPHAN_AGE_S" ] && continue
    log "service $s (created $created) is not known to the controller, deleting it"
    kubectl delete ksvc "$s" -n $NS --wait=false >/dev/null
  done
}

rev_state() {  # "<Ready status> <reason>" of the latest created revision of a Knative service
  local rev
  rev=$(kubectl get ksvc "$1" -n $NS -o jsonpath='{.status.latestCreatedRevisionName}')
  [ -n "$rev" ] || { echo "Unknown none"; return; }
  kubectl get revision "$rev" -n $NS \
    -o jsonpath='{.status.conditions[?(@.type=="Ready")].status} {.status.conditions[?(@.type=="Ready")].reason}'
}

repair_revisions() {
  # Backup of the controller's own repair (the baseline image has none). A new revision only where the latest one
  # cannot recover by itself: its container exited (reason ExitCode...), or Knative gave up starting it
  # (ProgressDeadlineExceeded: there was no room for its pods) and now no pod is waiting for room. A revision whose
  # pods are still starting or find no room is left alone: a new revision would only start more pods.
  local s state
  for s in $(ksvcs); do
    state=$(rev_state "$s")
    case $state in
      False\ ExitCode*) ;;
      False\ ProgressDeadlineExceeded) kubectl get pods -n $NS --no-headers 2>/dev/null | grep -q ' Pending ' && continue ;;
      *) continue ;;
    esac
    log "revision of $s failed ($state), forcing a new one"
    kubectl patch ksvc "$s" -n $NS --type merge -p \
      "{\"spec\":{\"template\":{\"metadata\":{\"annotations\":{\"lsf.thesis/restarted-at\":\"$(date -u +%FT%TZ)\"}}}}}" >/dev/null
  done
}

pods_ok() {  # all d- pods Running and fully ready
  kubectl get pods -n $NS --no-headers 2>/dev/null | grep '^d-' | grep -v Terminating | \
    awk '{n++; split($2,a,"/"); if ($3!="Running" || a[1]!=a[2]) bad=1} END{exit (bad || n==0)}'
}

log_unready_pods() {  # which pods are not ready, and why a Pending one is not scheduled
  local p
  kubectl get pods -n $NS --no-headers | grep '^d-' | grep -v Terminating | \
    awk '{split($2,a,"/"); if ($3!="Running" || a[1]!=a[2]) print "  " $1, $2, $3}' | head -5 | tee -a "$LOG"
  p=$(kubectl get pods -n $NS --no-headers | awk '$1 ~ /^d-/ && $3=="Pending" {print $1; exit}')
  [ -n "$p" ] && kubectl get events -n $NS --field-selector involvedObject.name="$p" --no-headers 2>/dev/null \
    | tail -1 | cut -c1-220 | sed 's/^/  /' | tee -a "$LOG"
}

active_layout() {  # active_layout_key of the app, from the configurator API through the API server proxy
  app_json | python -c 'import sys, json; print(json.load(sys.stdin).get("active_layout_key", ""))' 2>/dev/null
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
  local attempt bad
  for attempt in 1 2 3; do
    bad=0
    uncordon_nodes
    remove_orphans
    ensure_es || { log "Elasticsearch still not reachable"; bad=1; }
    repair_revisions
    for _ in $(seq 36); do pods_ok && break; sleep 5; done
    pods_ok || { log "pods are not ready:"; log_unready_pods; bad=1; }
    wait_small_layout || bad=1
    disk_ok || { bad=1; sleep 60; }
    [ $bad = 0 ] && return 0
    log "environment check $attempt failed"
  done
  return 1
}

# --- configuration and one run ------------------------------------------------------------------

CURRENT_CONFIG=""
switch_config() {  # applies a configuration (folder name); configs/<config>/ keeps config.txt and the output
  [ "$CURRENT_CONFIG" = "$1" ] && return 0
  local dir=$ROOT/configs/$1
  mkdir -p "$dir"
  # After a run the controller switches back to the small layout and deletes the large layout's deployments
  # 10 s later; restarting it in between (Egon's baseline image) leaves their Knative services behind
  wait_small_layout && sleep 30
  log "config $1"
  case $1 in
    1-baseline-original)      RATE_FORECAST=false tools/measure/use_config.sh baseline "$dir" ;;
    2-full-prewarm)           RATE_FORECAST=false tools/measure/use_config.sh full "$dir" ;;
    3-proactive-early-switch) RATE_FORECAST=false tools/measure/use_config.sh proactive "$dir" ;;
    4-follow-rate)            RATE_FORECAST=false tools/measure/use_config.sh follow "$dir" ;;
    5-follow-rate-forecast)   RATE_FORECAST=true  tools/measure/use_config.sh follow "$dir" ;;
    5-follow-rate-forecast-spare-pods)  SPARE_PODS=single RATE_FORECAST=true tools/measure/use_config.sh follow "$dir" ;;
    5-follow-rate-forecast-backup-cold) BACKUP_LAYOUT=cold RATE_FORECAST=true tools/measure/use_config.sh follow "$dir" ;;
  esac > "$dir/use_config.log" 2>&1
  [ "$1" = 1-baseline-original ] && reset_baseline_min_scale
  CURRENT_CONFIG=$1
}

measure() {  # $1 run dir, $2 run id, $3 profile, $4 phases, $5 loadgen options, $6 fault delay, $7 fault command
  local D=$1 START END PW FP
  echo "$3" > "$D/profile.txt"
  kubectl get pods -n $NS -o wide | grep d- | grep -v Terminating > "$D/pods-before.txt"
  python tools/measure/podwatch.py --out "$D" & PW=$!
  if [ -n "$6" ]; then
    ( sleep "$6"; date -u +%Y-%m-%dT%H:%M:%S.%3NZ > "$D/fault.txt"; bash -c "$7" >> "$D/fault.txt" 2>&1 ) & FP=$!
  fi
  START=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  # shellcheck disable=SC2086  # the loadgen options are words
  python tools/measure/loadgen.py --host "$HOST" --profile "$3" --cameras 8 --run-id "$2" --out "$D" $5
  [ -n "$FP" ] && wait "$FP"   # the fault (e.g. a node back after 90 s) is over before the run ends
  sleep 30
  END=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  kill $PW 2>/dev/null
  kubectl logs -n configurator deploy/lsf-configurator-deployment --since-time="$START" > "$D/controller.log"
  ensure_es
  python tools/measure/collect_traces.py --app $APP --start "$START" --end "$END" --out "$D"
  python tools/measure/reconfig_breakdown.py --run "$D" --limit $LIMIT
  python tools/measure/plot_run.py --run "$D" --limit $LIMIT
  python tools/measure/plot_phases.py --run "$D" --limit $LIMIT --phases "$4" --title "${D#"$ROOT"/}"
  if [ -n "$6" ] || [ -n "$5" ]; then
    # shellcheck disable=SC2046  # no fault time for the overload without an injected fault
    python tools/measure/fault_timeline.py "$D" 10 $( [ -n "$6" ] && echo "$6 $LIMIT" ) > "$D/fault_timeline.txt"
  fi
  [ -s "$D/traces.csv" ] && [ -s "$D/phases.csv" ]
}

FAILS=0
run_one() {  # $1 run dir relative to ROOT, then the measure arguments from the profile on
  local rel=$1 D=$ROOT/$1; shift
  [ -f "$D/DONE" ] && { log "skip $rel (done)"; return; }
  if ! check_env; then
    FAILS=$((FAILS+1)); log "SKIP $rel: environment not clean ($FAILS in a row)"
    if [ $FAILS -ge 3 ]; then
      log "aborting after 3 failed checks; leaving the cluster in $FAULT_CONFIG"
      switch_config "$FAULT_CONFIG"; remove_orphans; write_summary; exit 1
    fi
    return
  fi
  FAILS=0
  rm -rf "$D"; mkdir -p "$D"
  cp "$ROOT/configs/$CURRENT_CONFIG/config.txt" "$D/config.txt" 2>/dev/null
  log "START $rel"
  sleep "$SETTLE"
  if measure "$D" "$(echo "$rel" | tr '/' '-')-$(date +%H%M)" "$@" > "$D/run_output.txt" 2>&1; then
    date -u +%FT%TZ > "$D/DONE"; log "END $rel"
  else
    log "FAILED $rel (see run_output.txt), will be repeated on the next start"
  fi
}

# --- day overview -------------------------------------------------------------------------------

write_readme() {
  local e
  {
    echo "Measurements of $(basename "$ROOT"). Latency limit $LIMIT ms, 8 cameras, $REPS repetitions (repN)."
    echo "Each run folder: phases.txt (per-phase table), plots/ (overview and per-phase plots), config.txt."
    echo
    echo "1-normal-operation/<profile>/<configuration>/"
    echo "  profiles:"
    echo "    ramp-4min      ${PROFILE[ramp-4min]}  (requests/s per segment: seconds:rate)"
    echo "    steps-24min    ${PROFILE[steps-24min]}"
    echo "  configurations:"
    echo "    1-baseline-original       Egon's original framework: cold deployment at the switch, switches on the latency"
    echo "    2-full-prewarm            every layout kept warm with all replicas, switches on the latency"
    echo "    3-proactive-early-switch  as 2, but switches as soon as the rate reaches the active layout's capacity"
    echo "    4-follow-rate             the warm pods follow the measured rate; switches up and down on the rate"
    echo "    5-follow-rate-forecast    as 4, sized for the forecast rate"
    echo
    echo "2-faults/ and 3-overload/: in the 5-follow-rate-forecast configuration. In 2-faults/ every fault has a"
    echo "without-/with- pair that differs in one protection only: with-spare-pods/ SPARE_PODS=single (a spare pod for"
    echo "the compositions planned for one pod), with-backup-layout/ BACKUP_LAYOUT=cold (the compositions move away"
    echo "from an unavailable node). 3-overload/ has no protection yet. fault.txt says when and what was injected,"
    echo "fault_timeline.txt per 10 s and the outcome of all requests."
    for e in "${EXPERIMENTS[@]}"; do
      printf '  %-80s %s\n' "$e" "${E_TEXT[$e]}"
      printf '  %-80s profile %s\n' "" "${E_PROFILE[$e]}"
    done
  } > "$ROOT/README.txt"
}

write_summary() {  # one line per run: the whole-run numbers of reconfig_breakdown, the lost requests of faults
  local f d
  g() { grep -m1 -E "^ +$1 " "$f" 2>/dev/null | awk '{print $2}'; }
  {
    printf '%-86s %6s %9s %8s %8s %8s %6s\n' run late in_limit% p95_ms p99_ms max_ms lost
    while read -r d; do
      f=$d/run_output.txt
      printf '%-86s %6s %9s %8s %8s %8s %6s\n' "${d#"$ROOT"/}" "$(g over_limit)" "$(g within_limit_pct)" \
        "$(g p95_ms)" "$(g p99_ms)" "$(g max_ms)" \
        "$(grep -m1 'but not completed' "$d/fault_timeline.txt" 2>/dev/null | awk '{print $(NF-1)}')"
    done < <(find "$ROOT" -name DONE -printf '%h\n' | sort)
  } > "$ROOT/summary.txt"
}

# --- main ---------------------------------------------------------------------------------------

if [ -n "$DRY" ]; then
  log "DRY: active layout = '$(active_layout)', d- services: $(ksvcs | tr '\n' ' ')"
  check_env && log "DRY: environment is clean" || log "DRY: environment is NOT clean"
  exit 0
fi

write_readme
log "campaign start: $REPS repetitions${ONLY:+, only $ONLY}"
wanted() { [ -z "$ONLY" ] || [[ "$1" == "$ONLY"* ]]; }

# 1. Normal operation: within a repetition every configuration once (both profiles after one switch); the
# configuration order is rotated in every repetition, so drifts of the machine do not hit only the last ones
n=${#CONFIGS[@]}
for rep in $(seq "$REPS"); do
  for k in $(seq 0 $((n-1))); do
    cfg=${CONFIGS[$(( (k + rep - 1) % n ))]}
    for prof in "${PROFILES[@]}"; do
      rel=1-normal-operation/$prof/$cfg/rep$rep
      wanted "$rel" || continue
      [ -f "$ROOT/$rel/DONE" ] && { log "skip $rel (done)"; continue; }
      switch_config "$cfg"
      run_one "$rel" "${PROFILE[$prof]}" "${PHASES[$prof]}" "" "" ""
    done
  done
done

# 2-3. Faults and overload, in the final configuration; the overload experiments last, they stress the cluster most
for e in "${EXPERIMENTS[@]}"; do
  for rep in $(seq "$REPS"); do
    rel=$e/rep$rep
    wanted "$rel" || continue
    [ -f "$ROOT/$rel/DONE" ] && { log "skip $rel (done)"; continue; }
    switch_config "${E_CONFIG[$e]}"
    run_one "$rel" "${E_PROFILE[$e]}" "${E_PHASES[$e]}" "${E_LOADGEN[$e]}" "${E_DELAY[$e]}" "${E_CMD[$e]}"
  done
done

uncordon_nodes
switch_config "$FAULT_CONFIG"   # leave the cluster without the protections, as the next campaign expects
write_summary
log "campaign finished"