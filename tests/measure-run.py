#!/usr/bin/env python3
"""One measured run of the pipeline, counted at every hop.

Launches the producer Job at RATE lines/s for TOTAL lines via
modules/02-producer-workload/run-producer.py, and counts delivery at
every hop:
  hop0 producer emitted        (per-pod PRODUCER_SUMMARY lines)
  hop1 Vector source received + sink sent   (collector GraphQL API)
  hop2 Logstash pipeline in / out           (Logstash monitoring API)
  hop3 Loki stored distinct seq for RUN_ID  (count-run.py)
Prints one RESULT JSON object.
Delivered % = loki_distinct_seq / producer_emitted.

Usage: measure-run.py RUN_ID RATE TOTAL [settle_seconds]
Environment: see _hops.py and count-run.py (SOURCE_CONTEXT for the
cluster running the producer/collector, SINK_CONTEXT for the cluster
running Logstash/Loki; leave both unset for a single cluster).
"""
import json
import os
import re
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from _hops import (delta, logstash_stats, vector_sink, vector_source)  # noqa: E402

RUN_ID = sys.argv[1]
RATE = sys.argv[2]
TOTAL = sys.argv[3]
SETTLE = int(sys.argv[4]) if len(sys.argv) > 4 else 45

LAUNCHER = os.path.join(HERE, "..", "modules", "02-producer-workload",
                        "run-producer.py")


def loki_count():
    win = 2 if int(RATE) >= 100 else 30
    out = ""
    for _attempt in range(2):
        out = subprocess.run(
            [sys.executable, os.path.join(HERE, "count-run.py"),
             RUN_ID, str(win), "1300"],
            capture_output=True, text=True, timeout=900).stdout
        if "distinct_seq=" in out:
            break
        time.sleep(30)
    m = re.search(r"distinct_seq=(\d+).*missing_in_range=(\d+)", out)
    if m:
        return int(m.group(1)), int(m.group(2))
    return None, None


ls_in0, ls_out0 = logstash_stats()
vsrc0, vsink0 = vector_source(), vector_sink()
t0 = time.time()

launch = subprocess.run(
    [sys.executable, LAUNCHER, RUN_ID, "--rate", RATE, "--total", TOTAL],
    capture_output=True, text=True, timeout=1800)
print(launch.stdout.strip())
emitted = sum(int(m) for m in
              re.findall(r"PRODUCER_SUMMARY .* emitted=(\d+)", launch.stdout))

time.sleep(SETTLE)
ls_in1, ls_out1 = logstash_stats()
vsrc1, vsink1 = vector_source(), vector_sink()
distinct, missing = loki_count()

result = {
    "run_id": RUN_ID, "rate_target": int(RATE), "total_planned": int(TOTAL),
    "producer_emitted": emitted or None,
    "vector_source_delta": delta(vsrc1, vsrc0),
    "vector_sink_delta": delta(vsink1, vsink0),
    "logstash_in_delta": delta(ls_in1, ls_in0),
    "logstash_out_delta": delta(ls_out1, ls_out0),
    "loki_distinct_seq": distinct, "loki_missing_in_range": missing,
    "wall_s": round(time.time() - t0, 1),
}
print("RESULT " + json.dumps(result))
