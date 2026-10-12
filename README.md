# Logging pipeline lab: producer → Vector (OpenShift Logging) → Logstash → Loki + file archive

A small, measured log pipeline for OpenShift, packaged so each piece can
be consumed **module by module** on a different cluster:

```
producer Job ──stdout──► Vector collector DaemonSet ──HTTP──► Logstash ──┬─► Loki (queryable)
 (ns log-sim)            (OpenShift Logging operator,                      └─► plain-text files
                          ClusterLogForwarder)                               cluster/ns/pod/container/log.log
```

Built and measured in a single-node OpenShift lab: **lossless up to
5,000 lines/s (≈ 5 MB/s at 1 KB lines)**; above that, loss occurs at
exactly one place — the Vector collection hop — and is measured per hop,
not guessed. Full results and lessons: [docs/results.md](docs/results.md).

## Modules — take only what you need

| Module | What it is | Consume it by |
|---|---|---|
| [01-producer-image](modules/01-producer-image/) | Docker image that generates exact-size JSON log lines at a set rate, each line stamped with `run_id` + monotonic `seq` so delivery can be counted exactly | Build the Dockerfile anywhere; push to your registry or sideload onto nodes |
| [02-producer-workload](modules/02-producer-workload/) | Namespace, ConfigMap (rate/bytes/total only), single + parallel Jobs, and the launcher script | `kubectl apply`, then `python3 run-producer.py myrun1` |
| [03-collector-openshift-logging](modules/03-collector-openshift-logging/) | OpenShift Logging operator install + ClusterLogForwarder. The operator creates the **Vector DaemonSet**; the forwarder ships one namespace's logs to Logstash over HTTP | Edit the Logstash URL in `clf.yaml`, `kubectl apply` |
| [04-logstash](modules/04-logstash/) | Logstash deployment: HTTP input on 8080, Loki output **and** a plain-text file archive (`cluster/namespace/pod/container/log.log`) on a PVC | Edit the Loki URL / cluster name, `kubectl apply` |

## Tests — prove delivery, per hop

[tests/](tests/) counts what actually arrived, hop by hop
(producer emitted → Vector received/sent → Logstash in/out → distinct
`seq` stored in Loki). Start with one measured run:

```bash
python3 tests/measure-run.py myrun1 100 12000
# RESULT {... "producer_emitted": 12000, ..., "loki_distinct_seq": 12000 ...}
```

Then find your cluster's ceiling:

```bash
python3 tests/sweep.py sweep1 4 2500 60
```

Environment variables (`SOURCE_CONTEXT`, `SINK_CONTEXT`, `LOKI_URL`, …)
let the same scripts work whether Logstash/Loki live in the same cluster
as the producer or in a different one. See [tests/README.md](tests/README.md).

## Quickstart (single cluster)

> **Prerequisite — bring your own Loki.** This kit does not install
> Loki: module 04's Logstash *outputs* to a Loki gateway you already
> run (edit its URL in that module), and the tests count delivered
> lines in Loki. No Loki? Drop the `loki` output block from module 04
> and keep just the file archive — but then the Loki-based tests have
> nothing to count against; use `wc -l` on the archive files as ground
> truth instead.

```bash
# 1. Logstash sink (namespace observability)
kubectl create ns observability
kubectl apply -f modules/04-logstash/logstash.yaml

# 2. Collector: operator + forwarder (point clf.yaml at your Logstash first)
kubectl apply -f modules/03-collector-openshift-logging/install.yaml
oc -n openshift-logging get csv   # WAIT until cluster-logging shows Succeeded
kubectl apply -f modules/03-collector-openshift-logging/clf.yaml
kubectl -n openshift-logging get pods   # collector-* DaemonSet pods appear

# 3. Producer image: build + push to a registry your cluster can pull
docker build -t <your-registry>/log-producer:0.1.0 modules/01-producer-image/
docker push <your-registry>/log-producer:0.1.0
#    ...and set that image name in modules/02-producer-workload/producer-job*.yaml

# 4. Producer workload, then ONE measured run (measure-run launches
#    the producer itself and reconciles every hop)
kubectl apply -f modules/02-producer-workload/namespace.yaml
python3 tests/measure-run.py firstrun 100 12000
```

## Design notes worth stealing

- **ConfigMap holds workload knobs only** (`LINES_PER_SEC`, `LINE_BYTES`,
  `TOTAL_LINES`). CPU/memory live in the Job pod spec — requests only,
  **no limits** — because a measurement producer must be able to burst,
  and resource management is not workload configuration.
- **`run_id` + `seq` on every line** turns "are we losing logs?" into
  arithmetic: distinct seq at the sink vs emitted at the source.
- **The one big bug:** `logstash-output-loki` with an empty
  `include_fields` turns *every* event field into a Loki label; Vector's
  per-line `timestamp` then explodes the stream count and Loki silently
  drops pushes at its stream cap. The allowlist in
  [modules/04-logstash](modules/04-logstash/) is load-bearing — keep it.

## License

MIT — see [LICENSE](LICENSE).
