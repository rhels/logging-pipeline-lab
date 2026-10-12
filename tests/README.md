# Tests — measure delivery, hop by hop

"Are we losing logs?" is arithmetic here, not vibes. Every producer line
carries `run_id` + monotonic `seq`; the scripts reconcile counters at
each hop:

```
producer emitted → Vector source received → Vector sink sent
                 → Logstash in → Logstash out → distinct seq in Loki
```

The hop where *sent* first falls below what the previous hop received —
with that component's discard counter climbing — is the losing hop.

## Scripts

| Script | What it does |
|---|---|
| `count-run.py RUN_ID [window_s] [lookback_s]` | Exact delivered-line count for one run in Loki: total lines, distinct `seq`, missing-in-range. Queries in small time windows (wide windows over a dense burst time out). |
| `measure-run.py RUN_ID RATE TOTAL [settle_s]` | One full measured run through `run-producer.py`, with before/after counters at Vector + Logstash and the Loki distinct-seq count. Prints one `RESULT` JSON. |
| `sweep.py RUN_BASE PODS RATE_PER_POD SECONDS [settle_s]` | Ceiling sweep with parallel producers; sink-side reconciliation (Vector sink, Logstash, Loki), summed over pod partitions. |
| `_hops.py` | Shared counter helpers (Vector GraphQL API on the collector pod, Logstash monitoring API). |

## Examples

```bash
# smoke: 100 lines/s, 12,000 lines — expect 100% at every hop
python3 measure-run.py smoke1 100 12000

# find the ceiling: 4 pods x 2,500 lines/s for 60 s = 600,000 lines offered
python3 sweep.py sweep1 4 2500 60

# recount a past run (data is already in Loki)
python3 count-run.py smoke1
```

## Environment

Defaults assume producer, collector, Logstash, and Loki are all in your
current kubectl context. Override as needed:

| Var | Default | Meaning |
|---|---|---|
| `KUBECTL_BIN` | `kubectl` | kubectl binary |
| `SOURCE_CONTEXT` | current | context of the producer/collector cluster |
| `SINK_CONTEXT` | current | context of the Logstash/Loki cluster |
| `SINK_NAMESPACE` | `observability` | Logstash namespace |
| `LOGSTASH_DEPLOY` | `logstash` | Logstash deployment name |
| `COLLECTOR_NAMESPACE` | `openshift-logging` | collector namespace |
| `LOKI_URL` | unset | if set (e.g. `http://localhost:3100` via port-forward), count-run queries Loki directly; otherwise it execs through the Logstash pod to the in-cluster gateway |
| `LOKI_SELECTOR` | `{log_type="application"}` | LogQL stream selector (the label the shipped pipeline stamps on application logs); override if your Loki labels streams differently, e.g. `{service_name=~".+"}` |
| `LOKI_INCLUSTER_URL` | `http://loki-gateway/loki/api/v1/query_range` | gateway URL used by the exec-through-Logstash path when `LOKI_URL` is unset; change it if your gateway Service is not named `loki-gateway` |
| `VECTOR_SOURCE_MATCH` / `VECTOR_SINK_MATCH` | `logsim` / `logstash` | substrings matched against Vector component IDs from your CLF |

Prerequisites: the counter helpers `kubectl exec` into the collector
and Logstash pods and call `curl` inside them — the shipped Logstash
image and the OpenShift collector image both carry `curl`. Setting
`LOKI_URL` (a port-forward to your Loki gateway) avoids the
Logstash-exec path for counting. The Vector counters are summed across
all collector pods, so multi-node clusters count correctly.

Expect ~+2 duplicate entries per run at the sink (Vector is
at-least-once): distinct-`seq` counting is the source of truth, raw line
counts run slightly high.
