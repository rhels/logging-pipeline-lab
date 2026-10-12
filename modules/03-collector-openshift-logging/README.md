# Module 03 — collector: OpenShift Logging operator + ClusterLogForwarder

This is the collection tier. Installing the OpenShift Logging operator
and applying a ClusterLogForwarder (CLF) gives you a **Vector DaemonSet**
— one collector pod per node (`collector-*` in `openshift-logging`) —
that tails pod logs and forwards the namespaces you select.

As shipped, the CLF forwards **only the `log-sim` namespace's**
application logs, over HTTP, to Logstash (module 04).

## Files

| File | What it is |
|---|---|
| `install.yaml` | namespace + OperatorGroup + Subscription for OpenShift Logging 6.6 (channel `stable-6.6`) |
| `clf.yaml` | collector service account + binding, and the CLF: input = `log-sim` only, output = Logstash HTTP |

## Use

```bash
kubectl apply -f install.yaml
# wait for the operator:
oc -n openshift-logging get csv   # cluster-logging.v6.6.x, phase Succeeded

# FIRST edit the output URL in clf.yaml to a Logstash endpoint reachable
# from this cluster (in-cluster Service DNS name, or an external host).
kubectl apply -f clf.yaml
oc -n openshift-logging get clusterlogforwarder collector   # Ready=True
oc -n openshift-logging get pods                             # collector-* per node
oc -n openshift-logging get daemonset                        # the Vector collector DS
```

## Widen it beyond log-sim

Add namespaces to the input's `application.includes` list in `clf.yaml`,
or add further inputs/pipelines for infrastructure/audit logs. Keep the
lab scope until your sink is proven — forwarding everything into an
unmeasured sink is how silent loss starts.

## Notes from the lab

- CLF 6.6 namespace filtering uses `spec.inputs[].application.includes:
  [{namespace: ...}]` (an `application.namespaces` field is rejected by
  strict decoding).
- The generated Vector config exposes a Prometheus exporter on the
  collector pod (:24231) and a GraphQL API on 127.0.0.1:8686 — the test
  scripts in `tests/` use the GraphQL counters to measure per-hop loss.
- Collector resource defaults (operator-managed): requests 500m CPU /
  64Mi, limits 6 CPU / 2 Gi in the lab. The measured lossless ceiling
  (≈5,000 lines/s on an 8 vCPU single node) was a **per-line collection
  cost** ceiling, not a network one — see [docs/results.md](../../docs/results.md).
