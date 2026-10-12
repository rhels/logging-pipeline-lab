#!/usr/bin/env python3
"""Synthetic log producer for the logging pipeline lab.

Emits JSON log lines of an exact byte size to stdout at a configured rate.
Every line carries a run_id and a monotonic sequence number so the receiving
end (Logstash -> Loki) can count delivered vs sent exactly; gaps in seq at
the sink are dropped lines, by construction.

Env:
  LINES_PER_SEC  float, default 10     target emission rate
  LINE_BYTES     int,   default 1024   exact bytes per line incl. newline
  RUN_ID         str,   default auto   run identifier stamped on every line
  TOTAL_LINES    int,   default 0      0 = run until killed
  START_SEQ      int,   default 1

Start/summary lines go to stderr, so the producer's own progress never
pollutes the measured stdout stream.
"""
import json
import os
import signal
import sys
import time
import uuid


def env(name, default):
    v = os.environ.get(name)
    return default if v is None or v == "" else v


rate = float(env("LINES_PER_SEC", "10"))
line_bytes = int(env("LINE_BYTES", "1024"))
total = int(env("TOTAL_LINES", "0"))
seq = int(env("START_SEQ", "1"))
run_id = env("RUN_ID", "") or (
    "run-" + time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + "-" + uuid.uuid4().hex[:6]
)
host = os.environ.get("HOSTNAME", "unknown")

stop = False


def _sigterm(signum, frame):
    global stop
    stop = True


signal.signal(signal.SIGTERM, _sigterm)
signal.signal(signal.SIGINT, _sigterm)


def make_line(n):
    base = {"run_id": run_id, "seq": n, "ts_ms": int(time.time() * 1000),
            "host": host, "msg": ""}
    raw = json.dumps(base, separators=(",", ":")).encode()
    pad = line_bytes - 1 - len(raw)  # -1 for the trailing newline
    if pad < 0:
        raise ValueError("LINE_BYTES=%d too small for the JSON envelope" % line_bytes)
    base["msg"] = "x" * pad
    return (json.dumps(base, separators=(",", ":")) + "\n").encode()


out = sys.stdout.buffer
interval = 1.0 / rate if rate > 0 else 0.0
start = time.monotonic()
emitted = 0
print("PRODUCER_START run_id=%s rate=%s line_bytes=%d total=%d"
      % (run_id, rate, line_bytes, total), file=sys.stderr, flush=True)
while not stop and (total == 0 or emitted < total):
    due = start + emitted * interval
    now = time.monotonic()
    if due > now:
        time.sleep(min(due - now, 0.5))
        continue
    out.write(make_line(seq))
    seq += 1
    emitted += 1
    if emitted % 50 == 0:
        out.flush()
out.flush()
elapsed = max(time.monotonic() - start, 0.001)
print("PRODUCER_SUMMARY run_id=%s emitted=%d elapsed_s=%.2f achieved_rps=%.2f"
      % (run_id, emitted, elapsed, emitted / elapsed), file=sys.stderr, flush=True)
