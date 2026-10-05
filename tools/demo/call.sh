#!/bin/bash
# Sends one 2D or 3D request to the position-service deployment from a temporary
# in-cluster curl pod. The timestamp is taken inside the pod, right before the
# request, so the pod's startup time does not make it stale (ru-fusion expires
# fused objects older than 2 s).
#
# Usage: POS=<position deployment id> ./call.sh 2d|3d
# Prints the correlation id to pass to check.sh.

: "${POS:?POS is not set: export POS=<position deployment id, e.g. d-...>}"

KIND=$1
if [ "$KIND" != 2d ] && [ "$KIND" != 3d ]; then
  echo "Usage: POS=<position deployment id> $0 2d|3d" >&2
  exit 1
fi
CID="live-$KIND-$(date -u +%H%M%S)"

if [ "$KIND" = 2d ]; then
  DET='"detections":[{"class":"person","confidence":0.85,"bbox_2d":[420,370,450,460],"relative_position":{"distance_m":5.0,"direction_angle_rad":0.5,"direction_angle_deg":28.6}}]'
else
  DET='"detections":[{"track_id":1,"class":"person","location":[2.0,0.0,8.0],"dimensions":[0.5,0.5,1.8],"rotation_y":0.3,"bbox_2d":[420,370,450,460]}],"tracks":[{"track_id":1,"bbox_2d":[420,370,450,460]}]'
fi
META='"metadata":{"position":{"lat":47.4720386,"lon":19.059602,"alt":149.7},"heading":68.4}'

echo "correlation_id=$CID"
kubectl -n application run "curl-$KIND-$(date +%s)" --rm -i --restart=Never --quiet --image=curlimages/curl:8.10.1 --command -- sh -c "
TS=\$(date -u +%Y-%m-%dT%H:%M:%S+00:00); echo timestamp=\$TS
curl -s -w '\nHTTP %{http_code} in %{time_total}s\n' -X POST http://$POS.application.svc.cluster.local \
 -H 'Content-Type: application/json' -H 'X-Forward-To: position-service' -H 'X-Correlation-ID: $CID' \
 --data '{\"vehicle_id\":\"RSU-DEMO-01\",\"timestamp\":\"'\$TS'\",$DET,$META}'" 2>&1 | grep -v "If you don't see\|All commands and output\|couldn't attach"
