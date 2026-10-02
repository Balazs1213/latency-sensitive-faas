# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A thesis project on latency-sensitive FaaS on Kubernetes/Knative. An application is described as a call graph of small Python **components** (each a `<name>.py` exposing `def handler(event)`). The platform groups components into **function compositions** (one Knative service containing several components), places them on nodes, and rewires routing at runtime to keep end-to-end latency under a per-app limit.

Top-level layout:
- `lsf-configurator/` — the platform itself: Go backend (`main.go`, `api/`, `pkg/`), React/Vite/MUI frontend (`frontend/`), and the Python runtime template that every composition is built from (`templates/python/composition/`).
- `functions/` — composed functions being developed against that template (currently `position-ru-fusion`, an ADAS position/road-user-fusion pipeline ported from RabbitMQ microservices).
- `kubernetes/` — cluster setup (minikube, Istio, Knative, Tekton, OTel, Elastic/ECK, Redis). Scripts are Windows `.bat`/`.ps1`.
- `tools/` — benchmarker (per-handler profiling), JMeter eval plans + plotting scripts, sample deployment JSONs (`deployment-json/`).
- `misc/` — earlier prototypes (standalone load balancer, sample image-processing apps, feasibility tests). Mostly historical; the README's `loadbalanced-app` instructions refer to `misc/sample-apps/loadbalanced-app`.

## Commands

Configurator backend (Go 1.24, **requires CGO** for `mattn/go-sqlite3`):
```bash
cd lsf-configurator
CGO_ENABLED=1 go build -o lsf-configurator .
go vet ./...
```
There are no Go tests. Config comes from env vars / a `.env` file (see `pkg/config/env.go` for all keys and defaults, `example.env` for the minimum). `LOCAL_MODE=true` disables the latency controller loop. The server listens on `:8080` and serves the built frontend from `./public`.

Frontend:
```bash
cd lsf-configurator/frontend
npm install
npm run dev     # Vite dev server on :5173
npm run build   # tsc -b && vite build --mode dev
npm run lint
```

Container image / deploy (Windows): `lsf-configurator/deploy/deploy.bat` builds the multi-stage `Dockerfile` (Go + frontend + Python 3.11 with `networkx`/`slambuc`), pushes, applies the k8s manifests, and port-forwards to `localhost:8081`.

Python composition tests (run from inside the function/template dir, since modules import each other by bare name):
```bash
cd functions/position-ru-fusion        # or lsf-configurator/templates/python/composition
pytest test_func.py                     # unit tests for the func.py runtime
pytest test_func.py::test_name          # single test
python test_manual_chain.py             # manual handler tests
```
`test_manual*.py` are scripts, not pytest tests: they need a real Redis port-forwarded to `localhost:6379` (they set `REDIS_HOST`/`REDIS_PORT` before importing handlers). A local `venv/` is used in function dirs (gitignored).

## Architecture

### Configurator backend (`lsf-configurator/pkg`)
- `core/` holds domain models and interfaces (`models.go`, `api.go`, `repos.go`); concrete implementations live in sibling packages and are wired together in `main.go`. Keep this dependency direction — sibling packages import `core`, not the other way round.
- `core.Composer` (`composer.go`) is the orchestrator for FunctionApp → FunctionComposition → Deployment. Builds and deploys run asynchronously through a worker pool (`scheduler.go`) and return result channels.
- **Build path**: `bootstrapping/python.go` copies the template plus the selected component files into a build dir, verifies each has `def handler(`, merges `requirements.txt`, and **rewrites `config.py`** (inserts `from <comp> import handler as <comp>` and fills the `HANDLERS: Dict[...]` block). `builder/tekton.go` then creates a Tekton PipelineRun (buildpacks) which calls back via `NOTIFY_URL` → `Composer.NotifyBuildReady`.
- **Deploy path**: `knative/client.go` deploys via the `knative.dev/func` library with node affinity to `Deployment.Node`, injecting `FUNCTION_NAME` (= deployment id), `APP_NAME` (= app id) and the result-store address.
- **Routing**: `routing/configurator.go` writes each deployment's routing table as JSON into Redis under key = deployment id. Routes are either `"local"` (call the next component in-process) or `http://<deploymentId>.<namespace>.svc.cluster.local`. `kubeclient/dns.go` manages the app's entry DNS record.
- **Layout / autoscaling loop**: `core/scenarios.go` turns an app's components+links into a `LayoutScenario`; `layout/slambuc.go` shells out to `layout/slambuc_layout.py` (SLAMBUC `pseudo_ltree_partitioning`, JSON over stdin/stdout) to partition the call tree, then computes replicas/memory/CPU (`layout/calc.go`). Multiple layout candidates are precomputed per app. `core/controller.go` ticks, queries per-app latency (AVG or P95) from Elasticsearch traces (`metrics/`), and upgrades to a stricter layout when over `LatencyLimit` or downgrades when under `limit * LATENCY_DOWNGRADE_FACTOR` for many consecutive ticks (with cooldown).
- Persistence: SQLite (`data/db/schema.sql`, `data/repos`). Results: Redis (`results/`).
- HTTP API (`api/`): `/function_apps`, `/function_compositions`, `/deployments`, `/metrics`, `/results`, `/healthz`. The frontend uses `axios-case-converter`, so backend JSON is snake_case while TS models are camelCase.

### Python composition runtime (`templates/python/composition`)
Each composition is a Parliament (`python -m parliament .`) function. `func.py:main`:
1. Reads the target component from the `X-Forward-To` header and propagates `X-Correlation-ID`, OTel trace context, and `X-Request-Start-Time` (used to emit a synthetic `queue` span).
2. Loads this deployment's routing table from Redis (`config.read_config`, key = `FUNCTION_NAME`, host = `NODE_IP`).
3. Runs a work queue: invokes `HANDLERS[component](event)`; a handler may return a dict, bytes, or a list (= fan-out). For each output, `local` routes are enqueued in-process, remote routes are fire-and-forget POSTs (expected `ReadTimeout`), and a component with no outgoing routes writes the final result to Redis (`results.write_result`).
4. Spans tagged `trace_boundary_start` / `trace_boundary_end` are what the controller's latency metrics measure; don't drop them when changing the runtime.

The files in `functions/<name>/` are a copy of this template plus component modules with a hand-edited `config.py`. Runtime changes (`func.py`, `event.py`, `route.py`, `tracing.py`, ...) should normally be made in the template and mirrored, not diverge per function.

### `functions/position-ru-fusion`
Handlers `position-service-2d`, `position-service-3d` → `ru-fusion-service`, converted from RabbitMQ consumers by stripping the queue wrapping and keeping the pure calculation + Redis state logic. They use their own Redis client (`REDIS_HOST`/`REDIS_PORT`, separate from the routing-table Redis via `NODE_IP`) and optionally import a `monitoring.tsdb_client` module that isn't in this repo (they degrade gracefully when it's missing). `ru_fusion_handler` only runs fusion for RSU-sourced observations (vehicle id containing `rsu`).
