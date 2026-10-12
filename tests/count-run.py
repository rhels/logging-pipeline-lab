#!/usr/bin/env python3
"""Count delivered producer lines for a run in Loki.

Every producer line carries run_id + seq, so delivery can be counted
exactly: distinct seq at the sink vs lines emitted at the source. Gaps in
seq are dropped lines, by construction. Queries run in time windows to
stay under Loki's per-query entry limit.

Usage: count-run.py RUN_ID [window_seconds] [lookback_seconds]

Environment:
  LOKI_URL          Base URL of a Loki gateway reachable from THIS
                    machine, e.g. http://localhost:3100 (port-forward).
                    If set, Loki is queried directly over HTTP.
                    If unset, the query runs through `kubectl exec` into
                    the Logstash pod (which can reach the gateway
                    in-cluster) — the original lab arrangement.
  LOKI_SELECTOR     LogQL stream selector, default
                    {log_type="application"} (the label the shipped
                    pipeline stamps on application logs). If your Loki
                    labels streams differently (e.g. by service_name),
                    override it, e.g. {service_name=~".+"}.
  SINK_CONTEXT      kubectl context of the cluster running
                    Logstash/Loki (default: current context).
  SINK_NAMESPACE    Namespace of Logstash, default observability.
  LOGSTASH_DEPLOY   Deployment name of Logstash, default logstash.
  KUBECTL_BIN       kubectl binary, default kubectl.
"""
import json
import os
import re
import subprocess
import sys
import time
import urllib.parse
import urllib.request

run_id = sys.argv[1]
win = int(sys.argv[2]) if len(sys.argv) > 2 else 120
lookback = int(sys.argv[3]) if len(sys.argv) > 3 else 1800

LOKI_URL = os.environ.get("LOKI_URL", "").rstrip("/")
SELECTOR = os.environ.get("LOKI_SELECTOR", '{log_type="application"}')
KUBECTL = os.environ.get("KUBECTL_BIN", "kubectl")
SINK_NS = os.environ.get("SINK_NAMESPACE", "observability")
SINK_CTX = os.environ.get("SINK_CONTEXT", "")
LOGSTASH_DEPLOY = os.environ.get("LOGSTASH_DEPLOY", "logstash")
INCLUSTER_LOKI = os.environ.get(
    "LOKI_INCLUSTER_URL", "http://loki-gateway/loki/api/v1/query_range")


def kc_base():
    cmd = [KUBECTL]
    if SINK_CTX:
        cmd += ["--context", SINK_CTX]
    return cmd


def fetch_window(t0, t1):
    query = f'{SELECTOR} |= "{run_id}"'
    if LOKI_URL:
        params = urllib.parse.urlencode({
            "query": query,
            "start": f"{t0}000000000",
            "end": f"{t1}000000000",
            "limit": "5000",
        })
        with urllib.request.urlopen(
                f"{LOKI_URL}/loki/api/v1/query_range?{params}",
                timeout=280) as resp:
            return resp.read().decode()
    args = kc_base() + [
        "-n", SINK_NS, "exec", f"deploy/{LOGSTASH_DEPLOY}", "--",
        "curl", "-s", "-G", INCLUSTER_LOKI,
        "--data-urlencode", f"query={query}",
        "--data-urlencode", f"start={t0}000000000",
        "--data-urlencode", f"end={t1}000000000",
        "--data-urlencode", "limit=5000",
    ]
    return subprocess.run(args, capture_output=True, text=True,
                          timeout=280).stdout


end = int(time.time())
start = end - lookback
seqs = set()
total_lines = 0
t = start
while t < end:
    w_end = min(t + win, end)
    out = fetch_window(t, w_end)
    try:
        d = json.loads(out)
        for stream in d["data"]["result"]:
            for _ts, line in stream["values"]:
                total_lines += 1
                m = re.search(r'"seq":\s*(\d+)', line)
                if m:
                    seqs.add(int(m.group(1)))
    except Exception as e:
        print(f"window {t}-{w_end}: parse error {e}: {out[:120]}",
              file=sys.stderr)
    t = w_end

if seqs:
    lo, hi = min(seqs), max(seqs)
    missing = [s for s in range(lo, hi + 1) if s not in seqs]
    print(f"run={run_id} lines_returned={total_lines} distinct_seq={len(seqs)} "
          f"range={lo}..{hi} missing_in_range={len(missing)}")
    if missing:
        print("first missing:", missing[:20])
else:
    print(f"run={run_id} lines_returned={total_lines} distinct_seq=0")
