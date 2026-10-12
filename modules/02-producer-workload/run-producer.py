#!/usr/bin/env python3
"""Config-driven launcher for the log producer.

The workload knobs live in the producer-config ConfigMap (namespace
log-sim, see producer-config.yaml in this directory): LINES_PER_SEC,
LINE_BYTES, TOTAL_LINES, consumed by the producer container via envFrom.
CPU/memory are not configuration — they are set in the Job pod spec
(requests only, no limits, so the producer can burst to free node
capacity) and are not handled here.

Usage:
  run-producer.py RUN_ID [--pods N]
                         [--rate R] [--total T] [--bytes B]
                         [--no-wait]

Overrides patch the ConfigMap before launch, and the ConfigMap persists
between runs (this script creates it from producer-config.yaml only if it
does not exist yet — it never resets it). Edit it with kubectl or by
editing producer-config.yaml and re-applying that file yourself.
RUN_ID stays a launch argument: it is a per-run identity used for exact
delivery counting at the sink, not configuration.
With --pods N (>1) the Indexed parallel Job is used and TOTAL_LINES is
per pod; each pod's RUN_ID is <RUN_ID>-<index>.

Prints the effective config, applies the Job, waits for completion, and
prints each pod's PRODUCER_SUMMARY line.

Environment:
  KUBECTL_BIN   kubectl binary to use (default: "kubectl" from PATH).
                Your KUBECONFIG / current context is respected as-is.
"""
import json
import os
import re
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
KUBECTL = [os.environ.get("KUBECTL_BIN", "kubectl"), "--request-timeout=25s"]


def kc(*args, timeout=120):
    return subprocess.run(KUBECTL + list(args), capture_output=True, text=True,
                          timeout=timeout)


def parse_args(argv):
    opts = {"pods": 1, "wait": True}
    keys = {"--rate": "LINES_PER_SEC", "--total": "TOTAL_LINES",
            "--bytes": "LINE_BYTES"}
    overrides = {}
    run_id = None
    i = 0
    while i < len(argv):
        a = argv[i]
        if a in keys:
            overrides[keys[a]] = argv[i + 1]
            i += 2
        elif a == "--pods":
            opts["pods"] = int(argv[i + 1])
            i += 2
        elif a == "--no-wait":
            opts["wait"] = False
            i += 1
        elif not a.startswith("--"):
            run_id = a
            i += 1
        else:
            raise SystemExit(f"unknown argument: {a}")
    if not run_id:
        raise SystemExit(__doc__)
    return run_id, opts, overrides


def main():
    run_id, opts, overrides = parse_args(sys.argv[1:])

    # 1. Ensure the ConfigMap exists (create from the shipped defaults only
    #    if missing — never reset an existing one), then apply any overrides.
    out = kc("-n", "log-sim", "get", "configmap", "producer-config")
    if out.returncode != 0:
        out = kc("apply", "-f", f"{HERE}/producer-config.yaml")
        if out.returncode != 0:
            raise SystemExit("create producer-config failed: " + out.stderr.strip())
    if overrides:
        patch = json.dumps({"data": overrides})
        out = kc("-n", "log-sim", "patch", "configmap", "producer-config",
                 "--type", "merge", "-p", patch)
        if out.returncode != 0:
            raise SystemExit("patch producer-config failed: " + out.stderr.strip())

    # 2. Read the effective config back from the cluster (source of truth).
    out = kc("-n", "log-sim", "get", "configmap", "producer-config", "-o", "json")
    cfg = json.loads(out.stdout)["data"]
    print("CONFIG " + json.dumps(cfg))

    # 3. Render the Job template: only RUN_ID (+ pod count) is substituted.
    if opts["pods"] > 1:
        tpl = open(f"{HERE}/producer-job-parallel.yaml").read()
        job = tpl.replace("${RUN_BASE}", run_id).replace("${PODS}", str(opts["pods"]))
    else:
        tpl = open(f"{HERE}/producer-job.yaml").read()
        job = tpl.replace("${RUN_ID}", run_id)
    path = f"/tmp/producer-{run_id}.yaml"
    open(path, "w").write(job)
    out = kc("apply", "-f", path)
    print(out.stdout.strip() or out.stderr.strip())
    if out.returncode != 0:
        raise SystemExit("apply job failed")

    if not opts["wait"]:
        return

    # 4. Wait for completion, then print each pod's summary (job logs alone
    #    return only one pod of an Indexed Job).
    rate = float(cfg["LINES_PER_SEC"])
    total = int(cfg["TOTAL_LINES"])
    budget = int(total / rate) + 180 if rate > 0 and total > 0 else 900
    deadline = time.time() + min(max(budget, 240), 1500)
    status = None
    while time.time() < deadline:
        out = kc("-n", "log-sim", "get", "job", f"producer-{run_id}", "-o", "json")
        st = json.loads(out.stdout).get("status", {})
        if st.get("succeeded"):
            status = "Complete"
            break
        if st.get("failed"):
            status = "Failed"
            break
        time.sleep(5)
    print("JOB_STATUS " + (status or "TIMED_OUT"))
    pods = kc("-n", "log-sim", "get", "pods", "-l", f"run-id={run_id}",
              "--no-headers").stdout
    for line in pods.splitlines():
        name = line.split()[0]
        logs = kc("-n", "log-sim", "logs", name, "--tail=3").stdout
        for m in re.finditer(r"PRODUCER_SUMMARY.*", logs):
            print(f"{name}: {m.group(0)}")


if __name__ == "__main__":
    main()
