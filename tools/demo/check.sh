#!/bin/bash
# Checks the outcome of one call made with call.sh: the newest result:<app id>
# entry, the fused object in the ADAS Redis, and the perf:<app id> entry with
# the call's correlation id. Run it right after the call: a fused object is
# removed by the next ru-fusion call once it is older than 2 s.
#
# Usage: APP=<app id> ./check.sh <correlation_id>
# With an empty routing table on the position deployment (no fusion), the
# newest result has no fused_id, so this script reports an error on purpose;
# inspect it with: redis-cli LRANGE result:$APP -1 -1

: "${APP:?APP is not set: export APP=<function app id>}"
CID=${1:?Usage: APP=<app id> $0 <correlation_id>}

RM=$(kubectl -n redis get pods -o name | grep redis-master)
R=$(kubectl -n redis exec $RM -- redis-cli LRANGE result:$APP -1 -1)
FID=$(echo "$R" | python3 -c "import sys,json;e=json.loads(sys.stdin.read())['event'];print(e['positions'][0]['fused_id'] if e.get('positions') else '')")
echo "$R" | python3 -c "
import sys,json;r=json.loads(sys.stdin.read());e=r['event']
print('result: written', r['timestamp'], '| event.timestamp', e['timestamp'], '| fused_object_count', e.get('fused_object_count'), '| fused_id', [p.get('fused_id') for p in e['positions']], '| track_id', [p.get('observers',{}).get('RSU-DEMO-01',{}).get('track_id') for p in e['positions']])"
echo "fused object in ADAS Redis: $(kubectl -n default exec deploy/redis -- redis-cli EXISTS fused:obj:$FID) (fused:obj:$FID)"
echo "result list length: $(kubectl -n redis exec $RM -- redis-cli LLEN result:$APP)"
echo "perf match: $(kubectl -n redis exec $RM -- redis-cli LRANGE perf:$APP 0 -1 | grep -c "\"$CID\"") -> $(kubectl -n redis exec $RM -- redis-cli LRANGE perf:$APP 0 -1 | grep "\"$CID\"")"
