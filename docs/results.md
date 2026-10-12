# Lab results and lessons (2026-10-10/11)

Measured on a single-node OpenShift lab (8 vCPU / 18 GB node running a
full single-node cluster, ~88 pods) with Logstash + Loki on a second
machine. Producer lines were 1 KB JSON. Counting was exact
(distinct `seq` in Loki vs emitted at the producer).

## Delivery vs offered rate

| Offered rate | Lines | Delivered |
|---|---|---|
| 10 /s | 1,200 | 100% |
| 100 /s | 12,000 | 100% |
| 500 /s | 60,000 | 100% |
| 1,000 /s | 120,000 | 100% |
| 2,000 /s | 120,000 | 100% |
| **5,000 /s** | **300,000** | **100% — max lossless rate** |
| 7,500 /s | 450,000 | 76.1% |
| 10,000 /s | 600,000 | 60.1% |

On the lossy runs every producer pod emitted 100% of its lines, and
Vector-sink = Logstash = Loki counts matched exactly: **the loss is
entirely at Vector collection on the source cluster** (its forwarding
saturates ≈5,700–6,000 lines/s under burst). Expect ~+2 duplicate
entries per run (Vector at-least-once); distinct-seq counting is the
source of truth.

## The ceiling is per-line cost, not bandwidth

Vector's HTTP sink and the Logstash→Loki output already batch — that
removes per-*request* cost, which is why every downstream hop kept up
even at 10 MB/s offered. Batching does not remove per-*line* cost (tail,
parse, metadata enrichment), which is where the ceiling sits. To push
past it: more collector CPU, sharded collection, cheaper parsing — or
fewer, larger lines at the source.

## The one big bug: Loki label explosion

`logstash-output-loki` with an empty `include_fields` turns **every**
top-level event field into a Loki label. Vector adds a per-line
`timestamp`, so every line created a new stream: ~10,700 junk streams
hit Loki's `max_global_streams_per_user` (5,000, chart default), Loki
answered 429 "maximum active stream limit exceeded", and the plugin
retried then **silently dropped batches** — an hour of 100% loss with
only WARN logs. Fix (shipped in module 04):
`include_fields => ["log_type", "log_source", "hostname", "level"]`.

## Is a middle (aggregator) tier insurance or a leak?

The pipeline was also run both ways — collector direct to Logstash vs
collector → a standalone Vector aggregator tier (3 replicas, disk
buffer) → Logstash. The aggregator discarded **zero** events at
5,000 / 7,500 / 10,000 lines/s and *raised* delivered rates
(88.5→100%, 74.2→100%, 61.3→88.1%) by absorbing the backpressure that
otherwise lands on collection. Its cost is latency under overload
(buffer drain time), and connection-level load pinning across replicas
is real. Verdict: **insurance, not a leak** — if its buffer is on disk.

## Checking any tier for loss (recipe)

1. Snapshot per-component counters before and after a peak window:
   source `received_events_total`, sink `sent_events_total`, and
   `component_discarded_events_total` (Prometheus endpoint or the
   GraphQL API), per replica and summed, for every tier.
2. Reconcile hop by hop: the hop where sent < received of the previous
   hop, with discards climbing, is the losing hop. Discards at peak =
   saturation; buffer growth during a downstream outage that drains
   afterwards with zero discards = buffering working as insurance.
3. Check the per-replica split: long-lived connections balance
   per-connection, not per-event, so one replica can be pinned while
   others idle — aggregate counters alone can hide that.
4. Ground truth at the sink (distinct-seq count in Loki, or `wc -l` on
   the plain-text files) closes the loop; counters alone are claims.

## A source-side trap worth knowing

Kubelet rotates CRI logs as `0.log` (live) plus timestamped rotated
files; older rotated files are **gzip-compressed** — only the latest
stays plain. File-based collectors (Vector's file source included)
cannot decompress `.gz`, so a collector two or more rotations behind
silently loses whole files. With aggressive rotation settings
(e.g. 50 MB × 5 files) and hot containers, the per-container source
window can be minutes — size rotation and collector lag accordingly.
