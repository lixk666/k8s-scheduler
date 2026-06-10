import logging
from typing import Dict, Optional, Tuple

import requests
from kubernetes import client
from kubernetes.client import ApiException

from load_aware_scheduler.config import SchedulerConfig
from load_aware_scheduler.models import MetricsSnapshot, NodeInfo
from load_aware_scheduler.quantity import parse_cpu_cores, parse_memory_bytes

LOGGER = logging.getLogger(__name__)


class MetricsProvider:
    def __init__(self, cfg: SchedulerConfig, custom_api: client.CustomObjectsApi):
        self.cfg = cfg
        self.custom_api = custom_api

    def collect(self, nodes: Dict[str, NodeInfo]) -> MetricsSnapshot:
        if self.cfg.prometheus_url:
            prom = self._collect_prometheus()
            if prom:
                return prom

        metrics_api = self._collect_metrics_api(nodes)
        if metrics_api:
            return metrics_api

        return MetricsSnapshot()

    def _collect_prometheus(self) -> Optional[MetricsSnapshot]:
        try:
            snapshot = MetricsSnapshot()
            snapshot.node_cpu_pct = self._prom_node_vector(self.cfg.node_cpu_query)
            snapshot.node_memory_pct = self._prom_node_vector(self.cfg.node_memory_query)
            snapshot.pod_cpu_cores = self._prom_pod_vector(self.cfg.pod_cpu_query, as_int=False)
            pod_memory = self._prom_pod_vector(self.cfg.pod_memory_query, as_int=True)
            snapshot.pod_memory_bytes = {key: int(value) for key, value in pod_memory.items()}
            LOGGER.info(
                "loaded prometheus metrics: nodes(cpu=%s memory=%s) pods(cpu=%s memory=%s)",
                len(snapshot.node_cpu_pct),
                len(snapshot.node_memory_pct),
                len(snapshot.pod_cpu_cores),
                len(snapshot.pod_memory_bytes),
            )
            return snapshot
        except Exception as exc:
            LOGGER.warning("failed to collect Prometheus metrics: %s", exc)
            return None

    def _prom_query(self, query: str) -> list:
        response = requests.get(
            f"{self.cfg.prometheus_url}/api/v1/query",
            params={"query": query},
            timeout=self.cfg.prometheus_timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        if payload.get("status") != "success":
            raise RuntimeError(f"Prometheus query failed: {payload}")
        return payload.get("data", {}).get("result", [])

    def _prom_node_vector(self, query: str) -> Dict[str, float]:
        values: Dict[str, float] = {}
        for item in self._prom_query(query):
            metric = item.get("metric", {})
            node = (
                metric.get(self.cfg.prometheus_node_label)
                or metric.get("node")
                or metric.get("kubernetes_node")
                or metric.get("instance", "").split(":")[0]
            )
            if not node:
                continue
            values[node] = float(item["value"][1])
        return values

    def _prom_pod_vector(self, query: str, as_int: bool) -> Dict[Tuple[str, str], float]:
        values: Dict[Tuple[str, str], float] = {}
        for item in self._prom_query(query):
            metric = item.get("metric", {})
            namespace = metric.get("namespace")
            pod = metric.get("pod")
            if not namespace or not pod:
                continue
            raw = float(item["value"][1])
            values[(namespace, pod)] = int(raw) if as_int else raw
        return values

    def _collect_metrics_api(self, nodes: Dict[str, NodeInfo]) -> Optional[MetricsSnapshot]:
        try:
            snapshot = MetricsSnapshot()
            node_metrics = self.custom_api.list_cluster_custom_object(
                "metrics.k8s.io", "v1beta1", "nodes"
            )
            for item in node_metrics.get("items", []):
                name = item["metadata"]["name"]
                node = nodes.get(name)
                if not node:
                    continue
                usage = item.get("usage", {})
                cpu = parse_cpu_cores(usage.get("cpu"))
                memory = parse_memory_bytes(usage.get("memory"))
                if node.allocatable_cpu_cores > 0:
                    snapshot.node_cpu_pct[name] = 100.0 * cpu / node.allocatable_cpu_cores
                if node.allocatable_memory_bytes > 0:
                    snapshot.node_memory_pct[name] = 100.0 * memory / node.allocatable_memory_bytes

            pod_metrics = self.custom_api.list_cluster_custom_object(
                "metrics.k8s.io", "v1beta1", "pods"
            )
            for item in pod_metrics.get("items", []):
                namespace = item["metadata"]["namespace"]
                name = item["metadata"]["name"]
                cpu_total = 0.0
                memory_total = 0
                for container in item.get("containers", []):
                    usage = container.get("usage", {})
                    cpu_total += parse_cpu_cores(usage.get("cpu"))
                    memory_total += parse_memory_bytes(usage.get("memory"))
                snapshot.pod_cpu_cores[(namespace, name)] = cpu_total
                snapshot.pod_memory_bytes[(namespace, name)] = memory_total

            LOGGER.info(
                "loaded metrics-server metrics: nodes=%s pods=%s",
                len(snapshot.node_cpu_pct),
                len(snapshot.pod_cpu_cores),
            )
            return snapshot
        except ApiException as exc:
            if exc.status != 404:
                LOGGER.warning("failed to collect metrics.k8s.io metrics: %s", exc)
            return None
        except Exception as exc:
            LOGGER.warning("failed to collect metrics.k8s.io metrics: %s", exc)
            return None
