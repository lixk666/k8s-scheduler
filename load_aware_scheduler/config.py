import os
from dataclasses import dataclass
from typing import List, Optional


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return float(raw)


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


def _env_csv(name: str, default: List[str]) -> List[str]:
    raw = os.getenv(name)
    if raw is None:
        return list(default)
    return [item.strip() for item in raw.split(",") if item.strip()]


@dataclass(frozen=True)
class ScoreWeights:
    cpu_idle: float = 0.35
    memory_idle: float = 0.20
    app_spread: float = 0.15
    request_headroom: float = 0.10
    stability: float = 0.10
    node_type: float = 0.10


@dataclass(frozen=True)
class SchedulerConfig:
    scheduler_name: str
    namespace: Optional[str]
    poll_interval_seconds: float
    cache_refresh_seconds: float
    dry_run: bool
    strict_unsupported_constraints: bool
    log_node_scores: bool
    log_node_score_limit: int
    log_rejected_nodes: bool
    dashboard_enabled: bool
    dashboard_host: str
    dashboard_port: int
    dashboard_history_size: int

    prometheus_url: Optional[str]
    prometheus_timeout_seconds: float
    prometheus_node_label: str
    node_cpu_query: str
    node_memory_query: str
    pod_cpu_query: str
    pod_memory_query: str

    workload_label_keys: List[str]
    role_label_keys: List[str]
    app_label_keys: List[str]
    node_type_label_keys: List[str]

    cpu_hard_limit_pct: float
    memory_hard_limit_pct: float
    cpu_target_limit_pct: float
    memory_target_limit_pct: float
    cpu_limit_estimate_ratio: float
    memory_limit_estimate_ratio: float
    use_generic_profile_for_new_apps: bool
    request_fit_ratio: float
    failure_event_interval_seconds: int
    weights: ScoreWeights

    @classmethod
    def from_env(cls) -> "SchedulerConfig":
        weights = ScoreWeights(
            cpu_idle=_env_float("WEIGHT_CPU_IDLE", 0.35),
            memory_idle=_env_float("WEIGHT_MEMORY_IDLE", 0.20),
            app_spread=_env_float("WEIGHT_APP_SPREAD", 0.15),
            request_headroom=_env_float("WEIGHT_REQUEST_HEADROOM", 0.10),
            stability=_env_float("WEIGHT_STABILITY", 0.10),
            node_type=_env_float("WEIGHT_NODE_TYPE", 0.10),
        )

        namespace = os.getenv("WATCH_NAMESPACE", "").strip() or None
        prometheus_url = os.getenv("PROMETHEUS_URL", "").strip().rstrip("/") or None

        return cls(
            scheduler_name=os.getenv("SCHEDULER_NAME", "load-aware-scheduler"),
            namespace=namespace,
            poll_interval_seconds=_env_float("POLL_INTERVAL_SECONDS", 3.0),
            cache_refresh_seconds=_env_float("CACHE_REFRESH_SECONDS", 15.0),
            dry_run=_env_bool("DRY_RUN", False),
            strict_unsupported_constraints=_env_bool(
                "STRICT_UNSUPPORTED_CONSTRAINTS", False
            ),
            log_node_scores=_env_bool("LOG_NODE_SCORES", True),
            log_node_score_limit=_env_int("LOG_NODE_SCORE_LIMIT", 0),
            log_rejected_nodes=_env_bool("LOG_REJECTED_NODES", False),
            dashboard_enabled=_env_bool("DASHBOARD_ENABLED", True),
            dashboard_host=os.getenv("DASHBOARD_HOST", "0.0.0.0"),
            dashboard_port=_env_int("DASHBOARD_PORT", 8080),
            dashboard_history_size=_env_int("DASHBOARD_HISTORY_SIZE", 50),
            prometheus_url=prometheus_url,
            prometheus_timeout_seconds=_env_float("PROMETHEUS_TIMEOUT_SECONDS", 2.0),
            prometheus_node_label=os.getenv("PROMETHEUS_NODE_LABEL", "node"),
            node_cpu_query=os.getenv(
                "PROM_NODE_CPU_QUERY",
                '100 * sum by (node) (rate(container_cpu_usage_seconds_total{container!="",pod!=""}[5m])) '
                '/ sum by (node) (kube_node_status_allocatable{resource="cpu"})',
            ),
            node_memory_query=os.getenv(
                "PROM_NODE_MEMORY_QUERY",
                '100 * sum by (node) (container_memory_working_set_bytes{container!="",pod!=""}) '
                '/ sum by (node) (kube_node_status_allocatable{resource="memory"})',
            ),
            pod_cpu_query=os.getenv(
                "PROM_POD_CPU_QUERY",
                'sum by (namespace,pod) (rate(container_cpu_usage_seconds_total{container!="",pod!=""}[15m]))',
            ),
            pod_memory_query=os.getenv(
                "PROM_POD_MEMORY_QUERY",
                'sum by (namespace,pod) (container_memory_working_set_bytes{container!="",pod!=""})',
            ),
            workload_label_keys=_env_csv(
                "WORKLOAD_LABEL_KEYS",
                [
                    "bigdata/workload",
                    "workload-type",
                    "app.kubernetes.io/part-of",
                ],
            ),
            role_label_keys=_env_csv(
                "ROLE_LABEL_KEYS",
                [
                    "bigdata/role",
                    "spark-role",
                    "component",
                    "app.kubernetes.io/component",
                ],
            ),
            app_label_keys=_env_csv(
                "APP_LABEL_KEYS",
                [
                    "bigdata/app-id",
                    "spark-app-selector",
                    "sparkoperator.k8s.io/app-name",
                    "flinkdeployment.flink.apache.org/name",
                    "app.kubernetes.io/instance",
                    "app",
                ],
            ),
            node_type_label_keys=_env_csv(
                "NODE_TYPE_LABEL_KEYS",
                [
                    "bigdata/node-type",
                    "node.kubernetes.io/instance-type",
                    "beta.kubernetes.io/instance-type",
                ],
            ),
            cpu_hard_limit_pct=_env_float("CPU_HARD_LIMIT_PCT", 90.0),
            memory_hard_limit_pct=_env_float("MEMORY_HARD_LIMIT_PCT", 90.0),
            cpu_target_limit_pct=_env_float("CPU_TARGET_LIMIT_PCT", 80.0),
            memory_target_limit_pct=_env_float("MEMORY_TARGET_LIMIT_PCT", 85.0),
            cpu_limit_estimate_ratio=_env_float("CPU_LIMIT_ESTIMATE_RATIO", 0.50),
            memory_limit_estimate_ratio=_env_float("MEMORY_LIMIT_ESTIMATE_RATIO", 0.70),
            use_generic_profile_for_new_apps=_env_bool(
                "USE_GENERIC_PROFILE_FOR_NEW_APPS", True
            ),
            request_fit_ratio=_env_float("REQUEST_FIT_RATIO", 1.0),
            failure_event_interval_seconds=_env_int(
                "FAILURE_EVENT_INTERVAL_SECONDS", 60
            ),
            weights=weights,
        )
