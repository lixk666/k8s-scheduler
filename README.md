# load-aware-scheduler

`load-aware-scheduler` is a small Kubernetes scheduler for Flink and Spark
compute Pods. It watches Pending Pods whose `spec.schedulerName` is
`load-aware-scheduler`, scores nodes with real CPU/memory load plus app spread,
then binds the Pod through the Kubernetes Binding API.

It is meant for TaskManager and executor style Pods. Keep JobManager, Spark
driver, Pods with local PV requirements, and Pods with strict topology rules on
the default scheduler unless you have tested those constraints.

## What It Does

- Uses Prometheus metrics when `PROMETHEUS_URL` is configured.
- Falls back to `metrics.k8s.io` from metrics-server.
- Falls back again to request-based scoring when runtime metrics are missing.
- Respects basic Kubernetes constraints:
  - node readiness and unschedulable state
  - `nodeSelector`
  - required `nodeAffinity`
  - `NoSchedule` and `NoExecute` taints/tolerations
  - CPU and memory request fit
- Scores nodes with:
  - actual CPU idle
  - actual memory idle
  - same app spread
  - request headroom
  - stability margin
  - optional node type match

## Build

On macOS, build and push a Linux amd64 image for production nodes:

```bash
docker buildx build \
  --platform linux/amd64 \
  -t 10.14.2.6:8091/bigdata/load-aware-scheduler:v0.1.6 \
  --push \
  .
```

Or use the helper script:

```bash
./scripts/build-amd64.sh
```

Then deploy:

```bash
kubectl apply -k manifests
```

## Configure

Important environment variables:

```text
SCHEDULER_NAME=load-aware-scheduler
WATCH_NAMESPACE=
PROMETHEUS_URL=http://prometheus-operated.monitoring.svc:9090
CACHE_REFRESH_SECONDS=15
POLL_INTERVAL_SECONDS=3
CPU_HARD_LIMIT_PCT=90
MEMORY_HARD_LIMIT_PCT=90
CPU_TARGET_LIMIT_PCT=80
MEMORY_TARGET_LIMIT_PCT=85
CPU_LIMIT_ESTIMATE_RATIO=0.50
MEMORY_LIMIT_ESTIMATE_RATIO=0.70
USE_GENERIC_PROFILE_FOR_NEW_APPS=true
STRICT_UNSUPPORTED_CONSTRAINTS=false
LOG_NODE_SCORES=true
LOG_NODE_SCORE_LIMIT=0
LOG_REJECTED_NODES=false
DASHBOARD_ENABLED=true
DASHBOARD_PORT=8080
DASHBOARD_HISTORY_SIZE=50
```

## Dashboard

The scheduler exposes a read-only dashboard on port `8080`.

```bash
kubectl -n load-aware-scheduler port-forward svc/load-aware-scheduler-dashboard 8080:8080
```

Open `http://localhost:8080`.

JSON endpoints:

```text
/api/nodes
/api/schedules
/healthz
```

The default Prometheus queries assume `container_cpu_usage_seconds_total`,
`container_memory_working_set_bytes`, and `kube_node_status_allocatable`.
Override `PROM_NODE_CPU_QUERY`, `PROM_NODE_MEMORY_QUERY`, `PROM_POD_CPU_QUERY`,
and `PROM_POD_MEMORY_QUERY` if your metric labels differ.

## Use With Spark

```bash
spark-submit \
  --conf spark.kubernetes.scheduler.name=load-aware-scheduler \
  --conf spark.kubernetes.executor.label.bigdata/workload=spark \
  --conf spark.kubernetes.executor.label.bigdata/role=executor \
  --conf spark.kubernetes.executor.label.bigdata/rebalance=enabled \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.shuffleTracking.enabled=true \
  --conf spark.decommission.enabled=true \
  --conf spark.storage.decommission.enabled=true \
  --conf spark.storage.decommission.shuffleBlocks.enabled=true \
  ...
```

## Use With Flink

Add the scheduler name and labels through your Flink Pod template:

```yaml
apiVersion: v1
kind: Pod
metadata:
  labels:
    bigdata/workload: flink
    bigdata/rebalance: enabled
spec:
  schedulerName: load-aware-scheduler
```

Use it first on TaskManager Pods. Keep JobManager Pods on the default scheduler
until you intentionally decide otherwise.

## Production Notes

This scheduler binds Pods directly. It intentionally implements a focused subset
of scheduler predicates suitable for Flink TaskManagers and Spark executors. It
does not fully reproduce kube-scheduler behavior for PVC zone binding,
inter-Pod affinity, host ports, or strict topology spread. Set
`STRICT_UNSUPPORTED_CONSTRAINTS=true` to make it refuse Pods that use those hard
constraints.

For existing hot nodes, pair this scheduler with a separate rebalance controller
that deletes only safe TaskManager/executor Pods in small batches after checking
Flink checkpoint or Spark decommission readiness.
