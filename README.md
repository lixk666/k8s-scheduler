# load-aware-scheduler

`load-aware-scheduler` 是一个面向 Flink / Spark 计算型 Pod 的轻量级
Kubernetes 调度器。它监听 `spec.schedulerName=load-aware-scheduler` 的
Pending Pod，读取 Prometheus 或 metrics-server 中的实时资源指标，对候选
Node 打分，然后通过 Kubernetes Binding API 将 Pod 绑定到最合适的节点。

这个项目适合用在 **Flink TaskManager**、**Spark executor** 这类可重建的
计算 Pod 上。Flink JobManager、Spark driver、带本地存储/PVC 强约束、或者
严格 topology 约束的 Pod，建议先继续使用默认 scheduler，确认策略后再接入。

## 能解决什么

- Kubernetes 默认 scheduler 主要按 request 进行调度，无法感知真实 CPU/内存负载。
- Flink / Spark Pod 的实际资源使用差异很大，容易出现 Pod 数量看似均衡但节点实际负载不均衡。
- 很多大数据集群为了提高部署密度，会把 request 设置得很低，例如 `100m`，但 limit 可能是 `2C` 甚至更高。
- 新任务没有历史画像时，单纯按低 request 会严重低估资源消耗。

本调度器会综合考虑：

- Node 实际 CPU / 内存使用率
- Pod request
- Pod limit 的一定比例
- 同 app / 同 role 的实时资源画像
- 同 workload / 同 role 的通用资源画像
- 同 app 在各节点上的分散程度
- Node request 余量
- Node Ready / unschedulable / taint / nodeSelector / nodeAffinity

## 调度流程

整体流程如下：

```text
Pending Pod
   |
   v
只处理 spec.schedulerName=load-aware-scheduler 的 Pod
   |
   v
读取内存中的 ClusterSnapshot
   |
   v
对每个 Node 先做硬过滤
   |
   v
对剩余 Node 计算调度分
   |
   v
选择分数最高的 Node
   |
   v
调用 pods/binding 子资源完成绑定
```

调度器有两个循环：

```text
POLL_INTERVAL_SECONDS=3
```

表示每隔 3 秒扫描一次 Pending Pod。

```text
CACHE_REFRESH_SECONDS=15
```

表示每隔 15 秒刷新一次集群缓存，并查询 Prometheus / metrics-server。

Prometheus 查询压力主要由 `CACHE_REFRESH_SECONDS` 决定，而不是
`POLL_INTERVAL_SECONDS`。当前每次刷新大约执行 4 个 Prometheus instant query：

```text
node CPU
node memory
pod CPU
pod memory
```

生产集群 Pod 很多时，推荐先用：

```text
POLL_INTERVAL_SECONDS=3
CACHE_REFRESH_SECONDS=30
PROMETHEUS_TIMEOUT_SECONDS=2
```

如果 Prometheus 压力仍然较大，可以把 `CACHE_REFRESH_SECONDS` 调到 `60`。

## 硬过滤规则

Node 在打分前会先经过过滤。不满足以下条件的 Node 不参与排序：

- Node 必须 Ready
- Node 不能是 unschedulable
- 必须满足 Pod 的 `nodeSelector`
- 必须满足 required `nodeAffinity`
- Pod 必须容忍 Node 上的 `NoSchedule` / `NoExecute` taint
- Pod request 必须能放入 Node allocatable 剩余量
- 预估 CPU 不能超过 `CPU_HARD_LIMIT_PCT`
- 预估内存不能超过 `MEMORY_HARD_LIMIT_PCT`
- 如果 Pod 指定了 node type，必须和 Node 的 node type 匹配

默认硬限制：

```text
CPU_HARD_LIMIT_PCT=90
MEMORY_HARD_LIMIT_PCT=90
```

如果你不希望 Pod 被调度到内存接近 90% 的节点，可以调低：

```yaml
- name: MEMORY_HARD_LIMIT_PCT
  value: "88"
```

## 调度打分策略

真正决定 Pod 调到哪个 Node 的是 **调度分**。调度分是针对某个具体 Pending Pod
计算的，不是 Node 的全局固定分数。

默认公式：

```text
score =
  0.35 * CPU 空闲度
+ 0.20 * 内存空闲度
+ 0.15 * 分散度
+ 0.10 * request 余量
+ 0.10 * 稳定性
+ 0.10 * node type 匹配
```

对应环境变量：

```text
WEIGHT_CPU_IDLE=0.35
WEIGHT_MEMORY_IDLE=0.20
WEIGHT_APP_SPREAD=0.15
WEIGHT_REQUEST_HEADROOM=0.10
WEIGHT_STABILITY=0.10
WEIGHT_NODE_TYPE=0.10
```

### CPU 空闲度

```text
CPU 空闲度 = 100 - 预估 CPU 使用率
```

预估 CPU 使用率会把待调度 Pod 的估算 CPU 加进去：

```text
预估 CPU 使用率 = Node 当前 CPU 使用率 + Pod 估算 CPU / Node allocatable CPU
```

Node 当前 CPU 使用率优先来自 Prometheus；如果没有指标，会退回到 request 占比。

### 内存空闲度

```text
内存空闲度 = 100 - 预估内存使用率
```

预估内存使用率同样会把待调度 Pod 的估算内存加进去。

### 分散度

分散度不是只看同一个 app。当前是两层逻辑：

```text
第一层：同 app 分散度，key = app_id
第二层：同 workload + role 分散度，key = workload/role
```

同一个 app 在某个 Node 上的 Pod 越少，这个 Node 的同 app 分散度越高。

例如某个 Flink app 的 TaskManager 当前分布：

```text
node-a: 3 个
node-b: 1 个
node-c: 0 个
```

新 TaskManager 来时，`node-c` 会获得最高分散度分，`node-a` 最低。

但对于一个全新的 app，所有节点上的该 app Pod 数量通常都是 0：

```text
node-a: app_count = 0
node-b: app_count = 0
node-c: app_count = 0
```

这时同 app 分散度没有区分度，调度器会自动回退到 `workload + role` 维度，例如
`flink/taskmanager` 或 `spark/executor`：

```text
node-a: flink/taskmanager = 20 个
node-b: flink/taskmanager = 8 个
node-c: flink/taskmanager = 2 个
```

新 Flink TaskManager 会更倾向 `node-c`，因为该节点同类计算 Pod 更少。

如果同 app 分布和 workload/role 分布都有差异，调度器会组合两者：

```text
分散度 = 70% * 同 app 分散度 + 30% * workload/role 分散度
```

如果同 app 没有差异，只使用 workload/role 分散度。

这个权重默认是：

```text
WEIGHT_APP_SPREAD=0.15
```

历史上这个参数名叫 `WEIGHT_APP_SPREAD`，现在它控制的是整体分散度权重。  
如果你更希望打散同类 Flink/Spark 计算 Pod，可以调高它。如果你更关心 CPU 负载，
可以调低它并调高 `WEIGHT_CPU_IDLE`。

### Request 余量

```text
request 余量 = CPU request 余量和内存 request 余量的平均值
```

它用于避免把 Pod 放到 request 已经接近 allocatable 的节点上。

注意：在你们这种 request 普遍设置很低的集群里，这个指标只是辅助项，默认权重只有
`0.10`，不会成为主要决策依据。

### 稳定性

稳定性取 CPU 和内存两者里更紧张的那个余量：

```text
稳定性 = min(CPU 剩余空间, 内存剩余空间)
```

如果一个 Node CPU 很空但内存已经很紧张，稳定性分会被拉低。

### Node Type 匹配

如果 Pod 没有指定 node type，或者指定的 node type 与 Node 匹配，则该项得满分。

可识别的 node type label 默认包括：

```text
bigdata/node-type
node.kubernetes.io/instance-type
beta.kubernetes.io/instance-type
```

## Pod 资源估算逻辑

调度器不会简单使用 request，也不会简单使用完整 limit，而是使用混合估算。

当前公式：

```text
Pod 估算资源 = max(
  Pod request,
  Pod limit * limit 估算系数,
  同 app 同 role 实时画像 P75,
  同 workload 同 role 通用画像 P75
)
```

默认：

```text
CPU_LIMIT_ESTIMATE_RATIO=0.50
MEMORY_LIMIT_ESTIMATE_RATIO=0.70
USE_GENERIC_PROFILE_FOR_NEW_APPS=true
```

### 为什么不只看 request

如果你的 Pod 配置是：

```text
request cpu = 100m
limit cpu = 2C
```

但某些 Pod 运行时会接近 `2C`，那么第一次调度只按 `100m` 会明显低估，容易把 Pod
放到已经比较紧张的节点上。

### 为什么不完整使用 limit

如果 100 个 Pod 的 limit 都是 `2C`，完整按 limit 估算就相当于认为它们需要
`200C`。这会让调度非常保守，集群部署密度明显下降。

所以默认用 limit 的一部分：

```text
CPU:  limit * 0.50
内存: limit * 0.70
```

示例：

```text
request cpu = 100m
limit cpu = 2C
```

冷启动估算：

```text
max(100m, 2C * 0.50) = 1C
```

内存示例：

```text
request memory = 512Mi
limit memory = 4Gi
```

冷启动估算：

```text
max(512Mi, 4Gi * 0.70) = 2.8Gi
```

## 资源画像逻辑

画像是调度器内存中的实时画像，默认每次 cache refresh 重新生成。

画像来源：

```text
Prometheus / metrics-server 的 Pod CPU、内存实际使用量
```

画像分两层。

### 同 app 画像

Key：

```text
workload + app_id + role
```

例如：

```text
bigdata/workload=flink
bigdata/app-id=kenny-0819
bigdata/role=taskmanager
```

会形成画像：

```text
(flink, kenny-0819, taskmanager)
```

同一个 Flink app 的 TaskManager 会聚合到这个画像中。

如果当前同组已有 TaskManager 实际 CPU 为：

```text
0.4C, 0.8C, 1.2C, 2.0C
```

调度器会取 P75，例如大约 `1.2C`，并参与估算：

```text
Pod 估算 CPU = max(request, limit * ratio, 同 app P75)
```

### 通用画像

如果一个 app 是新任务，之前从没运行过，就没有同 app 画像。

这时调度器可以 fallback 到通用画像：

```text
workload + role
```

例如：

```text
(flink, taskmanager)
(spark, executor)
```

这能避免新任务完全只依赖 request 或 limit ratio。比如所有 Flink TaskManager
普遍会用到 `1.5C`，那么一个新 Flink app 的第一个 TaskManager 也可以参考这个
通用画像。

该行为由下面参数控制：

```text
USE_GENERIC_PROFILE_FOR_NEW_APPS=true
```

### 重启后的画像

画像没有落库，scheduler 重启后会丢失内存里的画像。

但重启后会立即执行一次 cache refresh：

```text
list nodes
查询 Prometheus / metrics-server
list running pods
按标签重新聚合画像
```

所以只要 Pod 仍在运行、指标可查，画像会在下一个刷新周期恢复。它恢复的是当前正在运行
Pod 的实时群体画像，不是跨天或跨小时的长期历史画像。

## 新任务冷启动行为

新任务如果没有同 app 画像，估算顺序是：

```text
request
limit * ratio
通用 workload + role 画像
```

所以在低 request 高 limit 的集群里，新任务不会只按 `100m` 估算。

推荐生产默认值：

```text
CPU_LIMIT_ESTIMATE_RATIO=0.50
MEMORY_LIMIT_ESTIMATE_RATIO=0.70
USE_GENERIC_PROFILE_FOR_NEW_APPS=true
```

如果调度仍然过于激进，可以提高：

```text
CPU_LIMIT_ESTIMATE_RATIO=0.60
MEMORY_LIMIT_ESTIMATE_RATIO=0.80
```

如果调度过于保守，可以降低：

```text
CPU_LIMIT_ESTIMATE_RATIO=0.35
MEMORY_LIMIT_ESTIMATE_RATIO=0.50
```

## 看板

调度器内置只读看板，端口为 `8080`。

```bash
kubectl -n load-aware-scheduler port-forward svc/load-aware-scheduler-dashboard 8080:8080
```

打开：

```text
http://localhost:8080
```

JSON endpoint：

```text
/api/nodes
/api/schedules
/healthz
```

看板中有两类分数：

### 基础分

节点资源表中的 **基础分** 是 Node 当前健康分，只看 Node 当前状态，不针对某个具体 Pod。

基础分考虑：

```text
CPU 空闲度
内存空闲度
request 余量
稳定性
Ready / unschedulable 状态
```

它用于观察节点整体健康程度，不直接决定某个 Pod 的调度结果。

### 最近调度评分

最近调度评分中的分数才是真正的 Pod 调度分。

它是针对某个具体 Pending Pod 计算的，会受以下因素影响：

```text
Pod request
Pod limit
Pod 同 app 画像
Pod 通用画像
Pod role
Pod app_id
nodeSelector
nodeAffinity
taints / tolerations
同 app 在该 Node 上已有多少 Pod
Node 当前 CPU / 内存
Node request 使用情况
```

所以可能出现：

```text
某个 Node 基础分不最高，但某个 Pod 仍然被调度过去
```

常见原因是分散度给了该 Node 更高的调度分：可能是同 app 更少，也可能是同
`workload/role` 的计算 Pod 更少。

## 日志

默认会打印每次调度的候选节点分数：

```text
LOG_NODE_SCORES=true
LOG_NODE_SCORE_LIMIT=0
```

`LOG_NODE_SCORE_LIMIT=0` 表示打印所有候选节点。大集群可以改成 `5`，只打印前 5 个。

如果想看被过滤节点的原因：

```text
LOG_REJECTED_NODES=true
```

## Prometheus 指标

默认 Prometheus 查询假设存在：

```text
container_cpu_usage_seconds_total
container_memory_working_set_bytes
kube_node_status_allocatable
```

可以通过环境变量覆盖：

```text
PROM_NODE_CPU_QUERY
PROM_NODE_MEMORY_QUERY
PROM_POD_CPU_QUERY
PROM_POD_MEMORY_QUERY
```

如果没有配置 `PROMETHEUS_URL`，调度器会 fallback 到 metrics-server 的
`metrics.k8s.io`。

## 构建

在 macOS 上构建生产 Linux amd64 镜像：

```bash
docker buildx build \
  --platform linux/amd64 \
  -t 10.14.2.6:8091/bigdata/load-aware-scheduler:v0.1.7 \
  --push \
  .
```

或者直接使用脚本：

```bash
./scripts/build-amd64.sh
```

部署：

```bash
kubectl apply -k manifests
```

## 配置

常用环境变量：

```text
SCHEDULER_NAME=load-aware-scheduler
WATCH_NAMESPACE=
PROMETHEUS_URL=http://prometheus-operated.monitoring.svc:9090
PROMETHEUS_TIMEOUT_SECONDS=2
CACHE_REFRESH_SECONDS=30
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

## Spark 接入

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

## Flink 接入

通过 Flink Pod template 设置 scheduler 和标签。

示例：

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: default-pod-template
  labels:
    bigdata/workload: flink
    bigdata/rebalance: enabled
spec:
  schedulerName: load-aware-scheduler
  containers:
    - name: flink-main-container
      resources:
        requests:
          cpu: 100m
          memory: 512Mi
          ephemeral-storage: 512Mi
        limits:
          cpu: "2"
          memory: 4Gi
          ephemeral-storage: 4096Mi
```

建议先只让 TaskManager 使用该 scheduler。JobManager 可以继续使用默认 scheduler，
等策略确认稳定后再决定是否接入。

## 生产注意事项

这个调度器是一个 focused scheduler，不是 kube-scheduler 的完整替代品。

它适合：

```text
Flink TaskManager
Spark executor
可删除重建的计算 Pod
```

谨慎接入：

```text
Flink JobManager
Spark driver
带 PVC / 本地盘强约束的 Pod
使用 required podAffinity / podAntiAffinity 的 Pod
使用 DoNotSchedule topologySpreadConstraints 的 Pod
使用 hostPort 的 Pod
```

如果希望严格拒绝这些复杂约束：

```text
STRICT_UNSUPPORTED_CONSTRAINTS=true
```

对于已经运行在热点节点上的旧 Pod，这个 scheduler 不会主动迁移。它只影响新创建的
Pending Pod。存量再平衡需要配合单独的 rebalance controller，或者人工小批量删除
安全的 TaskManager / executor，让它们重建后重新调度。
