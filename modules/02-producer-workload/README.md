# Module 02 — producer workload

Runs the producer image (module 01) as finite Jobs in the `log-sim`
namespace — the namespace the collector module forwards.

## Files

| File | What it is |
|---|---|
| `namespace.yaml` | the `log-sim` namespace |
| `producer-config.yaml` | ConfigMap `producer-config` — the workload knobs only |
| `producer-job.yaml` | single producer Job (template; `${RUN_ID}` substituted by the launcher) |
| `producer-job-parallel.yaml` | Indexed Job variant for high-rate runs; each pod emits its own partition `<RUN>-<index>`, `TOTAL_LINES` is per pod |
| `run-producer.py` | launcher: ensures/patches the ConfigMap, renders + applies the Job, waits, prints per-pod summaries |

## Configuration model

- The ConfigMap holds **only** `LINES_PER_SEC`, `LINE_BYTES`,
  `TOTAL_LINES`, consumed via `envFrom`.
- CPU/memory live in the Job pod specs as **requests only, no limits** —
  deliberate: a measurement producer should burst to whatever the node
  has free instead of being throttled by an artificial ceiling.
- `RUN_ID` is per-run identity, passed at launch — never configuration.

## Use

```bash
kubectl apply -f namespace.yaml

# one configured run (knobs default from the ConfigMap)
python3 run-producer.py myrun1

# override knobs for a run (they patch the ConfigMap, which persists)
python3 run-producer.py hot1 --rate 5000 --total 300000 --bytes 1024

# parallel producers for ceiling tests (TOTAL_LINES is per pod)
python3 run-producer.py sweep1 --pods 4 --rate 2500 --total 150000
```

Before the first run, set the image in `producer-job.yaml` /
`producer-job-parallel.yaml` to the registry path you pushed in
module 01. The specs ship `imagePullPolicy: IfNotPresent`: with a
registry image it pulls when missing; with a sideloaded node image
(short name, already in the node's image store) it uses the local copy.

Requires: `kubectl` on PATH with a context pointing at your cluster
(`KUBECTL_BIN` overrides the binary; your `KUBECONFIG` is respected).
