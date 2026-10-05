# Bemutató: position-service → ru-fusion-service Egon keretrendszerében

Ez a leírás lépésről lépésre végigvezet azon, hogyan mutatható be élőben, hogy a
`position-service` és a `ru-fusion-service` a platform saját útján fut:
configuratoron keresztül buildelve és kitelepítve, két külön kompozícióban,
HTTP-routinggal, és hogy a routing futás közben, újratelepítés nélkül
átkapcsolható.

Az itt szereplő parancsokat 2026-10-05-én a minikube klaszteren ellenőriztük
(`knative`, `knative-m02`, `knative-m03` node-ok). A hívó és ellenőrző scriptek:
`tools/demo/call.sh` és `tools/demo/check.sh`. A parancsokat a repó gyökeréből
futtasd.

Előfeltétel: az app már ki van telepítve a
`tools/deployment-json/position-ru-fusion.json` alapján (két kompozíció:
`position-service` a `knative-m02`-n, `ru-fusion-service` a `knative-m03`-on).

## 1. Változók

Az azonosítók minden új kitelepítésnél mások. Így derítheted ki őket:

```bash
RM=$(kubectl -n redis get pods -o name | grep redis-master)

# App id (a 2. lépésben indított port-forward kell hozzá)
curl -s localhost:8081/function_apps/ | python3 -c "import sys,json;[print(a['id'],a['name']) for a in json.load(sys.stdin) or []]"

# Deployment id-k: a routing table kulcsai. Amelyik értékében a
# "position-service" szerepel, az a position deployment; a másik ({}) a fusion.
kubectl -n redis exec $RM -- redis-cli --scan --pattern 'd-*'
kubectl -n redis exec $RM -- redis-cli GET <d-id>
```

Ezután állítsd be őket (a 2026-10-05-i kitelepítés értékei példaként):

```bash
export APP=9f4mt31got8dwfyzn4dppe9i8
export POS=d-claenypek3sebe7gevliafmvu
export FUS=d-58igs5x0f8sq4y8vwb1yylvww
RM=$(kubectl -n redis get pods -o name | grep redis-master)
```

## 2. A configurator API elérése

Egy külön terminálban indítsd el, és hagyd futni:

```bash
kubectl -n configurator port-forward svc/lsf-configurator 8081:80
```

Ellenőrzés:

```bash
curl -s localhost:8081/healthz          # várt kimenet: Ok
```

## 3. Elhelyezés és routing

```bash
kubectl -n application get ksvc                          # a két d-... szolgáltatás READY=True
kubectl -n application get pods -o wide | grep '^d-'     # két pod, két különböző node
kubectl -n redis exec $RM -- redis-cli GET $POS
kubectl -n redis exec $RM -- redis-cli GET $FUS
```

Mit kell látnod:

- `GET $POS` → `{"position-service":[{"component":"ru-fusion-service","url":"http://<FUS>.application.svc.cluster.local"}]}`, vagyis a position a fusiont HTTP-n hívja.
- `GET $FUS` → `{}`, vagyis a fusion az utolsó komponens, ő írja az eredményt.

## 4. NODE_IP

A pod a routing table-t a saját node-ján futó Redis replikából olvassa, a
`NODE_IP` címen. Ezt a változót a platform a Kubernetes downward API-n keresztül
adja át, és meg kell egyeznie a pod node-jának IP-jével:

```bash
P=$(kubectl -n application get pods -l serving.knative.dev/service=$POS -o name)
kubectl -n application exec $P -c user-container -- sh -c 'echo $NODE_IP'
kubectl -n application get $P -o jsonpath='{.status.hostIP}'; echo
```

A két kimenetnek azonosnak kell lennie (például `192.168.49.3`). A fusionnél
ugyanígy, `$FUS`-szal.

## 5. 2D és 3D hívás, ellenőrzés

```bash
tools/demo/call.sh 2d        # kiírja: correlation_id=..., timestamp=..., ok, HTTP 200
tools/demo/check.sh <correlation_id>
tools/demo/call.sh 3d
tools/demo/check.sh <correlation_id>
```

A `call.sh` egy ideiglenes curl podból hív (a pod magától törlődik), és az
időbélyeget a podon belül, közvetlenül a kérés előtt veszi fel. A `check.sh`-t
rögtön a hívás után futtasd: a fúzionált objektumot a következő fusion hívás
törli, ha az már 2 s-nál régebbi.

Mit kell látnod:

- a hívásnál `ok` és `HTTP 200`;
- `result:` → az `event.timestamp` megegyezik a küldött időbélyeggel, `fused_object_count 1`, és van `fused_id`;
- 3D hívásnál a `track_id` értéke `[1]` (ez csak a 3D ágból jöhet), 2D-nél `[None]`;
- `fused object in ADAS Redis: 1`;
- `perf match: 1`, a hívás correlation id-jával.

## 6. Logok

```bash
kubectl -n application logs -l serving.knative.dev/service=$POS -c user-container --timestamps --tail=20
kubectl -n application logs -l serving.knative.dev/service=$FUS -c user-container --timestamps --tail=20
kubectl -n application logs -l serving.knative.dev/service=$FUS -c queue-proxy --tail=50 | grep ERROR
```

Mit kell látnod:

- position: `Received 2d detection data …`, illetve 3D-nél `Received 3d detection data …`, utána `Processed …`;
- fusion: néhány ms-mal később egy új kérés (`No CloudEvent available`), majd `Fusing 1 observations from ego RSU-DEMO-01`;
- fusion queue-proxy: időnként `error reverse proxying request … context canceled`. Ez nem hiba, hanem a „fire-and-forget” továbbítás nyoma: a position pod 10 ms után bontja a kapcsolatot, a fusion viszont befejezi a munkát. Ha a fusion 10 ms-on belül végez (a 3D hívásnál ez előfordult), a sor nem jelenik meg.

A correlation id átjutását az is mutatja, hogy a `perf:` bejegyzést a fusion pod
írja (az időbélyege a fusion `write_result` idejével egyezik), és benne van a
hívás correlation id-ja. Ezt csak úgy tudhatja, ha az `X-Correlation-ID` header
átment a HTTP hívással.

## 7. Trace az Elasticsearchben

A jelszót ne írd le sehova: egy változóba olvasd ki a fürt secretjéből, és ne
írasd ki a képernyőre.

```bash
ES_PASS=$(kubectl -n observability get secret elasticsearch-es-elastic-user -o jsonpath='{.data.elastic}' | base64 -d)

kubectl -n observability exec elasticsearch-es-default-0 -c elasticsearch -- curl -s -u "elastic:$ES_PASS" \
 "http://localhost:9200/.ds-traces-apm*/_search?size=30&expand_wildcards=all&sort=@timestamp:asc&_source=@timestamp,span.name,service.name,trace.id,labels" \
 -H 'Content-Type: application/json' \
 -d '{"query":{"bool":{"filter":[{"term":{"labels.app_name":"'$APP'"}},{"range":{"@timestamp":{"gte":"now-2m"}}}]}}}'
```

Egy hívás spanjai egyetlen `trace.id`-val, ebben a sorrendben:

```
position  queue (trace_boundary_start)
position  read_config
position  position-service
position  forward_request
fusion    queue
fusion    read_config
fusion    ru-fusion-service
fusion    write_result (trace_boundary_end)
```

A `trace_boundary_start` és `trace_boundary_end` spanok között mért idő az, amit
a platform vezérlője késleltetésként figyel.

## 8. Routing-váltás futás közben, újratelepítés nélkül

A position deployment routing table-jét üresre állítjuk: így a position lesz az
utolsó komponens, és ő írja az eredményt, fúzió nélkül.

```bash
curl -s -X PUT -w 'HTTP %{http_code}\n' localhost:8081/deployments/$POS/routing_table \
 -H 'Content-Type: application/json' -d '{}'
kubectl -n redis exec $RM -- redis-cli GET $POS          # várt kimenet: {}

tools/demo/call.sh 2d
kubectl -n redis exec $RM -- redis-cli LRANGE result:$APP -1 -1
```

Mit kell látnod: az új `result:` bejegyzésben **nincs** `fused_object_count` és
`fused_id`, csak a nyers position-kimenet, és a fusion logjában nincs új
`Fusing …` sor. A `check.sh` ilyenkor szándékosan hibát jelez, mert nincs
`fused_id`.

Visszaállítás (a `function` mező a fusion deployment id-ja, ebből a
configurator maga képzi az URL-t):

```bash
curl -s -X PUT -w 'HTTP %{http_code}\n' localhost:8081/deployments/$POS/routing_table \
 -H 'Content-Type: application/json' \
 -d '{"position-service":[{"to":"ru-fusion-service","function":"'$FUS'"}]}'
kubectl -n redis exec $RM -- redis-cli GET $POS          # újra a http URL

tools/demo/call.sh 2d
tools/demo/check.sh <correlation_id>                       # újra fused_object_count 1
```

Az új tábla a következő kéréstől érvényes, mert a pod minden kérésnél újraolvassa.

Fontos korlát: `"function": "local"` csak akkor működik, ha a cél komponens
ugyanabban az image-ben van. A mostani két külön kompozícióval a `local` route
`HTTP 500`-at adna. Valódi helyi összevonáshoz egy harmadik,
`[position-service, ru-fusion-service]` kompozíció kellene, egy plusz builddel.

## 9. Mi van ellenőrizve, és mi még nincs

Élőben ellenőrizve (2026-10-05):

- a `handler(event)` szignatúra és a generált `config.py` (`"position-service" : position_service`, `"ru-fusion-service" : ru_fusion_service`) a futó podokban;
- a build a configuratoron keresztül (Tekton, két PipelineRun `Succeeded`) és a Knative deploy (két ksvc `Ready=True`, a megadott node-okon);
- a routing table HTTP route-tal, két pod két node-on, egy trace a két szolgáltatáson át;
- a `result:` és `perf:` írása, a correlation id továbbítása;
- a `NODE_IP` megegyezik a node IP-jével, és a routing table a node-lokális replikában is látszik;
- a trace boundary spanok az Elasticsearchben;
- a 2D/3D dispatch (`Received 2d…`, `Received 3d…`, 3D-nél `track_id: 1`);
- a futásidejű routing-váltás (`{}`, majd vissza) újratelepítés nélkül.

Még nincs ellenőrizve:

- a vezérlő terhelés alatt: fut és olvassa a trace-eket, de ennél az appnál még sosem váltott elrendezést (az app nem platform-managed, és nem volt terhelés);
- helyi (`local`) fúzió egyetlen image-ben élőben: csak helyben, a `func.py`-n keresztül (`functions/position-ru-fusion/test_local_e2e.py`);
- platform-managed regisztráció: a SLAMBUC eredményét (`[[1, 2]]`) csak offline, a szkripttel ellenőriztük;
- a `2d-detect-and-track` komponens: még nincs portolva;
- párhuzamosság és terhelés: nem volt terheléses teszt;
- ismert template-hibák: a `test_func.py` 3 tesztje bukik, és a `func.py` INFO logjai hiányoznak (lásd `CLAUDE.md`).

## 10. Takarítás a bemutató előtt

Ez csak a törlendő kulcsok listája. A parancsokat magad futtasd, ha tiszta
állapotból szeretnél indulni.

`redis-master`:

- `result:<APP>` és `perf:<APP>` – a futó app korábbi teszthívásai;
- `result:95b2uewzk9emz5doq3o2lr5zr` és `perf:95b2uewzk9emz5doq3o2lr5zr` – a már törölt régi app maradéka;
- `result:position-ru-fusion`, `perf:position-ru-fusion`, `position-ru-fusion-local` – a helyi tesztek kulcsai.

**Ne töröld** a `d-...` kulcsokat (`$POS`, `$FUS`): ezek a futó app routing table-jei.

ADAS Redis (`default/redis`):

- `fused:obj:*`, `fused:geo`, `fused:egos`, `fused:ego:RSU-DEMO-01`, `vehicle:RSU-DEMO-01`.

**Ne töröld** a `vehicle:506` kulcsot: ez a munkánk előtt is ott volt.
