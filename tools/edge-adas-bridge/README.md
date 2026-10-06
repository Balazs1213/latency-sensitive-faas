# Edge-ADAS bridges

Two small bridges connect the original queue-based edge-adas parts
(LavinMQ) with the two components that run inside the FaaS framework
(`position-service` and `ru-fusion-service`).

## Data flow

```
camera-stream (LavinMQ)
   -> 2d-detect-and-track            (original component)
   -> detection-tracking-results     (LavinMQ queue)
   -> Bridge 1                       (this folder)
   -> position-service               (FaaS framework, HTTP)
   -> ru-fusion-service              (FaaS framework)
   -> result:<app id>                (Redis list)
   -> Bridge 2                       (this folder)
   -> fusion-results-stream          (LavinMQ stream)
   -> original frontend
```

- **Bridge 1** (`bridge1.py`) reads every message from
  `detection-tracking-results` and sends it to `position-service` as an
  HTTP POST.
- **Bridge 2** (`bridge2.py`) watches the `result:<app id>` list in Redis
  and publishes every new fusion result to `fusion-results-stream`.

## Prerequisites

- The edge-adas stack runs in the `default` namespace (LavinMQ, Redis,
  `2d-detect-and-track`).
- `position-service` and `ru-fusion-service` are deployed in the FaaS
  framework (namespace `application`).
- The original `position-service` deployment in `default` must be scaled
  to 0, otherwise it competes with Bridge 1 for the same queue:

```
  kubectl scale deployment position-service --replicas=0 -n default
```

## Configuration

Both bridges run as plain Python pods. Their settings are environment
variables in the yaml files:

| File           | Variable       | Meaning                                            |
| -------------- | -------------- | -------------------------------------------------- |
| `bridge1.yaml` | `AMQP_URL`     | LavinMQ connection                                 |
| `bridge1.yaml` | `POSITION_URL` | In-cluster URL of the framework `position-service` |
| `bridge2.yaml` | `AMQP_URL`     | LavinMQ connection                                 |
| `bridge2.yaml` | `REDIS_HOST`   | Redis that holds the framework results             |
| `bridge2.yaml` | `RESULT_KEY`   | `result:<app id>` of the fusion application        |

`POSITION_URL` and `RESULT_KEY` contain generated ids. If the framework
applications are redeployed, both values must be updated.

## Start

Run from the repository root. The ConfigMap holds the code, the
deployment mounts it into the pod.

```
kubectl create configmap bridge1-code \
  --from-file=bridge1.py=tools/edge-adas-bridge/bridge1.py \
  -n default --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f tools/edge-adas-bridge/bridge1.yaml

kubectl create configmap bridge2-code \
  --from-file=bridge2.py=tools/edge-adas-bridge/bridge2.py \
  -n default --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f tools/edge-adas-bridge/bridge2.yaml
```

After changing a `.py` file, update the ConfigMap with the command above
and restart the pod, because the code is only read at startup:

```
kubectl rollout restart deployment bridge1 -n default
```

## Check

```
kubectl logs deployment/bridge1 -n default --tail=10
kubectl logs deployment/bridge2 -n default --tail=10
```

- Bridge 1 prints one line per message: `bridge-<id> -> HTTP 200`.
- Bridge 2 prints `Watching result:... ` at startup and one
  `published (N objects)` line per fusion result.

To feed the pipeline, run the offline scenario
(`edge-adas/offline-data/run_scenarios.py`), then open the original
frontend.

## Stop and restore

(venv) ubuntu@wdc-3:~/thesis/latency-sensitive-faas$ tail -8 tools/edge-adas-bridge/README.md

- Bridge 2 prints `Watching result:... ` at startup and one
  `published (N objects)` line per fusion result.

To feed the pipeline, run the offline scenario
(`edge-adas/offline-data/run_scenarios.py`), then open the original
frontend.

## Stop and restore

```
kubectl delete -f tools/edge-adas-bridge/bridge1.yaml
kubectl delete -f tools/edge-adas-bridge/bridge2.yaml
kubectl delete configmap bridge1-code bridge2-code -n default
kubectl scale deployment position-service --replicas=1 -n default
```

The last command restores the original `position-service`.
