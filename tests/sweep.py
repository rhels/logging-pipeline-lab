#!/usr/bin/env python3
"""Ceiling sweep for the pipeline.

Launches an Indexed producer Job (PODS pods, each RATE lines/s for
SECONDS) via modules/02-producer-workload/run-producer.py, then verifies
per hop: producer emitted (per-pod summaries), Vector sink delta,
Logstash in/out deltas, and the Loki distinct-seq total summed over all
pod partitions (<RUN>-0 .. <RUN>-N).

Usage: sweep.py RUN_BASE PODS RATE_PER_POD SECONDS [settle_s]
Environment: see _hops.py and count-run.py.
"""
import json
import os
import re
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
from _hops import (delta, logstash_stats, vector_sink)  # noqa: E402

RUN = sys.argv[1]
PODS = int(sys.argv[2])
RATE = int(sys.argv[3])
SECONDS = int(sys.argv[4])
SETTLE = int(sys.argv[5]) if len(sys.argv) > 5 else 45
TOTAL_PER_POD = RATE * SECONDS

LAUNCHER = os.path.join(HERE, "..", "modules", "02-producer-workload",
                        "run-producer.py")


def loki_partition_total():
    tot = 0
    ok = False
    for i in range(PODS):
        out = subprocess.run(
            [sys.executable, os.path.join(HERE, "count-run.py"),
             f"{RUN}-{i}", "30", "1300"],
            capture_output=True, text=True, timeout=900).stdout
        m = re.search(r"distinct_seq=(\d+)", out)
        if m:
            tot += int(m.group(1))
            ok = True
    return tot if ok else None


ls_in0, ls_out0 = logstash_stats()
vs0 = vector_sink()
t0 = time.time()

launch = subprocess.run(
    [sys.executable, LAUNCHER, RUN, "--pods", str(PODS),
     "--rate", str(RATE), "--total", str(TOTAL_PER_POD)],
    capture_output=True, text=True, timeout=3600)
print(launch.stdout.strip())
summaries = re.findall(
    r"PRODUCER_SUMMARY .* emitted=(\d+) .*achieved_rps=([\d.]+)",
    launch.stdout)
emitted = sum(int(e) for e, _r in summaries)
achieved = [float(r) for _e, r in summaries]

time.sleep(SETTLE)
ls_in1, ls_out1 = logstash_stats()
vs1 = vector_sink()
loki_total = loki_partition_total()

result = {
    "run": RUN, "pods": PODS, "rate_per_pod": RATE, "seconds": SECONDS,
    "target_total_rate": PODS * RATE, "planned": PODS * TOTAL_PER_POD,
    "producer_emitted": emitted or None,
    "producer_achieved_rps_each": achieved,
    "vector_sink_delta": delta(vs1, vs0),
    "logstash_in_delta": delta(ls_in1, ls_in0),
    "logstash_out_delta": delta(ls_out1, ls_out0),
    "loki_distinct_seq_total": loki_total,
    "wall_s": round(time.time() - t0, 1),
}
print("RESULT " + json.dumps(result))
