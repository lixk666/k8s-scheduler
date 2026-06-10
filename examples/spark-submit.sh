#!/usr/bin/env bash
set -euo pipefail

spark-submit \
  --master k8s://https://kubernetes.default.svc \
  --deploy-mode cluster \
  --conf spark.kubernetes.scheduler.name=load-aware-scheduler \
  --conf spark.kubernetes.driver.label.bigdata/workload=spark \
  --conf spark.kubernetes.driver.label.bigdata/role=driver \
  --conf spark.kubernetes.executor.label.bigdata/workload=spark \
  --conf spark.kubernetes.executor.label.bigdata/role=executor \
  --conf spark.kubernetes.executor.label.bigdata/rebalance=enabled \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.shuffleTracking.enabled=true \
  --conf spark.decommission.enabled=true \
  --conf spark.storage.decommission.enabled=true \
  --conf spark.storage.decommission.shuffleBlocks.enabled=true \
  local:///opt/spark/examples/jars/spark-examples.jar
