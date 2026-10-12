# Module 04 — Logstash sink

Logstash 8.17.3 as a Deployment in the `observability` namespace:

- **Input:** HTTP on port 8080, JSON codec — this is what the
  ClusterLogForwarder (module 03) posts to.
- **Output 1 — Loki:** via the `logstash-output-loki` plugin (installed
  at container start), pushing to a Loki gateway.
- **Output 2 — plain-text file archive:** one file per container on the
  `logstash-files` PVC, laid out as
  `<cluster>/namespace/pod/container/log.log`, containing just the log
  message lines — the flat, greppable copy.

Delete either output block in the ConfigMap if you only want one sink.

## Use

```bash
kubectl create ns observability   # if it does not exist yet
kubectl apply -f logstash.yaml
kubectl -n observability rollout status deploy/logstash
# smoke test (from a pod or via port-forward):
curl -X POST http://logstash.observability.svc.cluster.local:8080 \
  -H 'Content-Type: application/json' -d '{"message":"hello"}'
```

## Adapt before applying

1. **Loki URL** in `logstash.conf` — point it at your Loki gateway
   Service (full push path: `.../loki/api/v1/push`), or drop the loki
   output block.
2. **Cluster name** — the first segment of the file output path
   (`mycluster`) becomes the top folder of the archive.
3. **Exposure** — the Service is ClusterIP as shipped, which is right
   when collectors run in the *same* cluster. If collectors are in
   another cluster, expose Logstash with a Route / NodePort /
   LoadBalancer and use that address in the CLF output URL.
4. **PVC size** — 1 Gi was a lab size; size for your retention.

## Load-bearing detail

The `include_fields` allowlist on the loki output is **not optional**.
With it empty, the plugin turns every top-level event field into a Loki
label — including Vector's per-event `timestamp` — creating a new Loki
stream per line until Loki's active-stream limit starts silently
dropping pushes (HTTP 429). Keep labels tiny and constant:
`["log_type", "log_source", "hostname", "level"]`.

The startup command installs the loki plugin on **every** start, so
the pod needs outbound network access (or a mirror) at boot — in an
air-gapped cluster, pre-bake an image with the plugin installed
instead. Also note the plugin install step needs a bigger
JRuby heap (`LS_JAVA_OPTS="-Xmx2048m"` for the install only) or it OOMs;
runtime heap is 1 Gi with a 3 Gi pod memory limit.
