"""Shared per-hop counter helpers for measure-run.py and sweep.py.

Counters, not claims: each hop of the pipeline exposes how many events
it received / sent, so a loss can be localized to one hop instead of
guessed at.

  - Vector collector (OpenShift Logging): GraphQL API on the collector
    pod, localhost:8686. Source/sink components are matched by substring
    (env VECTOR_SOURCE_MATCH / VECTOR_SINK_MATCH) because the exact
    component IDs depend on your ClusterLogForwarder definition.
  - Logstash: monitoring API on localhost:9600, read via kubectl exec.

Environment: KUBECTL_BIN, SOURCE_CONTEXT, SINK_CONTEXT, SINK_NAMESPACE
(default observability), LOGSTASH_DEPLOY (default logstash),
COLLECTOR_NAMESPACE (default openshift-logging).
"""
import json
import os
import subprocess

KUBECTL = os.environ.get("KUBECTL_BIN", "kubectl")
SRC_CTX = os.environ.get("SOURCE_CONTEXT", "")
SINK_CTX = os.environ.get("SINK_CONTEXT", "")
SINK_NS = os.environ.get("SINK_NAMESPACE", "observability")
LOGSTASH_DEPLOY = os.environ.get("LOGSTASH_DEPLOY", "logstash")
COLL_NS = os.environ.get("COLLECTOR_NAMESPACE", "openshift-logging")
SRC_MATCH = os.environ.get("VECTOR_SOURCE_MATCH", "logsim")
SINK_MATCH = os.environ.get("VECTOR_SINK_MATCH", "logstash")


def _run(cmd, timeout=120):
    return subprocess.run(cmd, capture_output=True, text=True,
                          timeout=timeout).stdout


def _kc(ctx):
    cmd = [KUBECTL]
    if ctx:
        cmd += ["--context", ctx]
    return cmd


def logstash_stats():
    out = _run(_kc(SINK_CTX) + [
        "-n", SINK_NS, "exec", f"deploy/{LOGSTASH_DEPLOY}", "--",
        "curl", "-s", "localhost:9600/_node/stats/pipelines"], 60)
    d = json.loads(out)
    p = d["pipelines"]["main"]["events"]
    return p["in"], p["out"]


def _vector_graphql(kind, metric, match):
    # Sum across ALL collector pods: the collector is a DaemonSet, so on
    # a multi-node cluster each node has its own counters and reading
    # only the first pod would undercount.
    pods = _run(_kc(SRC_CTX) + ["-n", COLL_NS, "get", "pods",
                                "--no-headers"]).split()
    names = [x for x in pods if x.startswith("collector-")]
    if not names:
        return None
    gql = (f"query {{ {kind} {{ edges {{ node {{ componentId metrics "
           f"{{ {metric} {{ {metric} }} }} }} }} }} }}")
    total = None
    for name in names:
        out = _run(_kc(SRC_CTX) + [
            "-n", COLL_NS, "exec", name, "--", "sh", "-c",
            f"curl -s --max-time 6 -X POST localhost:8686/graphql "
            f"-H 'Content-Type: application/json' "
            f"-d '{{\"query\":\"{gql}\"}}'"], 60)
        try:
            for e in json.loads(out)["data"][kind]["edges"]:
                if match in e["node"]["componentId"]:
                    v = int(e["node"]["metrics"][metric][metric])
                    total = v if total is None else total + v
        except Exception:
            pass
    return total


def vector_source():
    return _vector_graphql("sources", "receivedEventsTotal", SRC_MATCH)


def vector_sink():
    return _vector_graphql("sinks", "sentEventsTotal", SINK_MATCH)


def delta(after, before):
    if after is None or before is None:
        return None
    return after - before
