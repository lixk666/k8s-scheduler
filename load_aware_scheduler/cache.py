import logging
import threading
import time
from collections import defaultdict
from typing import Dict, Optional

from kubernetes import client

from load_aware_scheduler.config import SchedulerConfig
from load_aware_scheduler.k8s_resources import (
    is_pod_terminal,
    node_type,
    pod_actual_usage,
    pod_effective_request,
    pod_identity,
)
from load_aware_scheduler.metrics import MetricsProvider
from load_aware_scheduler.models import ClusterSnapshot, NodeInfo, ResourceProfile
from load_aware_scheduler.quantity import parse_cpu_cores, parse_memory_bytes

LOGGER = logging.getLogger(__name__)


class ClusterCache:
    def __init__(
        self,
        cfg: SchedulerConfig,
        core_api: client.CoreV1Api,
        custom_api: client.CustomObjectsApi,
        stop_event: threading.Event,
    ):
        self.cfg = cfg
        self.core_api = core_api
        self.metrics_provider = MetricsProvider(cfg, custom_api)
        self.stop_event = stop_event
        self._snapshot: Optional[ClusterSnapshot] = None
        self._lock = threading.Lock()
        self._thread = threading.Thread(target=self._refresh_loop, daemon=True)

    def start(self) -> None:
        self.refresh()
        self._thread.start()

    def snapshot(self) -> ClusterSnapshot:
        with self._lock:
            if self._snapshot is None:
                raise RuntimeError("cluster cache is not initialized")
            return self._snapshot

    def assume(self, node_name: str, pod: object) -> None:
        identity = pod_identity(pod, self.cfg)
        request = pod_effective_request(pod)
        with self._lock:
            if self._snapshot:
                self._snapshot.assume(node_name, identity, request)

    def refresh(self) -> None:
        nodes = self._list_nodes()
        metrics = self.metrics_provider.collect(nodes)
        profiles: Dict[tuple, ResourceProfile] = defaultdict(ResourceProfile)

        pods = self._list_pods()
        for pod in pods:
            if is_pod_terminal(pod) or not pod.spec.node_name:
                continue
            node = nodes.get(pod.spec.node_name)
            if not node:
                continue

            identity = pod_identity(pod, self.cfg)
            request = pod_effective_request(pod)
            usage = pod_actual_usage(pod, metrics.pod_cpu_cores, metrics.pod_memory_bytes)

            node.requested_cpu_cores += request.cpu_cores
            node.requested_memory_bytes += request.memory_bytes
            if identity.app_id:
                node.app_counts[identity.app_id] = node.app_counts.get(identity.app_id, 0) + 1

            profile = profiles[identity.profile_key]
            if usage.cpu_cores is not None:
                profile.cpu_samples.append(usage.cpu_cores)
            if usage.memory_bytes is not None:
                profile.memory_samples.append(usage.memory_bytes)

            generic_profile = profiles[(identity.workload, "", identity.role)]
            if usage.cpu_cores is not None:
                generic_profile.cpu_samples.append(usage.cpu_cores)
            if usage.memory_bytes is not None:
                generic_profile.memory_samples.append(usage.memory_bytes)

        for name, node in nodes.items():
            node.actual_cpu_pct = metrics.node_cpu_pct.get(name)
            node.actual_memory_pct = metrics.node_memory_pct.get(name)

        snapshot = ClusterSnapshot(nodes=nodes, profiles=dict(profiles), metrics=metrics)
        with self._lock:
            self._snapshot = snapshot

        LOGGER.info(
            "cluster cache refreshed: nodes=%s pods=%s profiles=%s",
            len(nodes),
            len(pods),
            len(profiles),
        )

    def _refresh_loop(self) -> None:
        while not self.stop_event.wait(self.cfg.cache_refresh_seconds):
            try:
                self.refresh()
            except Exception:
                LOGGER.exception("cluster cache refresh failed")

    def _list_nodes(self) -> Dict[str, NodeInfo]:
        nodes: Dict[str, NodeInfo] = {}
        for node in self.core_api.list_node().items:
            labels = node.metadata.labels or {}
            allocatable = node.status.allocatable or {}
            ready = any(
                condition.type == "Ready" and condition.status == "True"
                for condition in node.status.conditions or []
            )
            nodes[node.metadata.name] = NodeInfo(
                name=node.metadata.name,
                labels=labels,
                taints=node.spec.taints or [],
                ready=ready,
                unschedulable=bool(node.spec.unschedulable),
                allocatable_cpu_cores=parse_cpu_cores(allocatable.get("cpu")),
                allocatable_memory_bytes=parse_memory_bytes(allocatable.get("memory")),
                node_type=node_type(labels, self.cfg),
            )
        return nodes

    def _list_pods(self) -> list:
        if self.cfg.namespace:
            return self.core_api.list_namespaced_pod(self.cfg.namespace).items
        return self.core_api.list_pod_for_all_namespaces().items
