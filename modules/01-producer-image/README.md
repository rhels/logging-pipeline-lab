# Module 01 — log producer image

A tiny synthetic log generator. It emits JSON lines of an **exact byte
size** to stdout at a configured rate. Every line carries a `run_id` and
a monotonic `seq`, so the receiving end can count delivered vs sent
exactly — gaps in `seq` at the sink are dropped lines, by construction.

Start/summary lines (`PRODUCER_START` / `PRODUCER_SUMMARY`) go to
**stderr**, so the producer's own progress never pollutes the measured
stdout stream.

## Environment (configuration)

| Var | Default | Meaning |
|---|---|---|
| `LINES_PER_SEC` | 10 | target emission rate (float) |
| `LINE_BYTES` | 1024 | exact bytes per line, including newline |
| `TOTAL_LINES` | 0 | lines to emit, then exit (0 = run until killed) |
| `RUN_ID` | auto | run identifier stamped on every line |
| `START_SEQ` | 1 | first sequence number |

## Build it

```bash
docker build -t log-producer:0.1.0 .
# or: podman build -t log-producer:0.1.0 .
docker push <your-registry>/log-producer:0.1.0
```

No registry? Sideload it onto the node(s) instead: the Job specs use
`imagePullPolicy: IfNotPresent`, so a same-named image already in the
node's image store is used without a pull — e.g. for a single-node lab
VM, `podman load` the saved image inside the VM.

## Try it without a cluster

```bash
docker run --rm -e LINES_PER_SEC=5 -e TOTAL_LINES=20 -e RUN_ID=smoke log-producer:0.1.0
```
