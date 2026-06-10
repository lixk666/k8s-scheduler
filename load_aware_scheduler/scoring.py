from typing import Dict, List, Tuple

from load_aware_scheduler.config import SchedulerConfig
from load_aware_scheduler.constraints import node_matches_required_constraints
from load_aware_scheduler.k8s_resources import (
    pod_effective_limit,
    pod_effective_request,
    pod_identity,
    pod_requested_node_type,
)
from load_aware_scheduler.models import ClusterSnapshot, NodeInfo, NodeScore, ResourceRequest
from load_aware_scheduler.quantity import clamp


class LoadAwareScorer:
    def __init__(self, cfg: SchedulerConfig):
        self.cfg = cfg

    def score_pod(self, pod: object, snapshot: ClusterSnapshot) -> Tuple[List[NodeScore], Dict[str, List[str]]]:
        identity = pod_identity(pod, self.cfg)
        request = pod_effective_request(pod)
        limit = pod_effective_limit(pod)
        estimated = self._estimated_request(
            identity.profile_key,
            request,
            limit,
            snapshot,
        )
        requested_node_type = pod_requested_node_type(pod, self.cfg)

        rejected: Dict[str, List[str]] = {}
        scores: List[NodeScore] = []
        app_counts = [node.app_counts.get(identity.app_id, 0) for node in snapshot.nodes.values()]
        min_app_count = min(app_counts) if app_counts else 0
        max_app_count = max(app_counts) if app_counts else 0

        for node in snapshot.nodes.values():
            ok, reasons = node_matches_required_constraints(
                pod, node, request, self.cfg.request_fit_ratio
            )
            if requested_node_type and node.node_type and requested_node_type != node.node_type:
                ok = False
                reasons.append(f"node type mismatch: want {requested_node_type}, got {node.node_type}")

            estimated_cpu_pct = self._estimated_cpu_pct(node, estimated)
            estimated_memory_pct = self._estimated_memory_pct(node, estimated)
            if estimated_cpu_pct >= self.cfg.cpu_hard_limit_pct:
                ok = False
                reasons.append(
                    f"estimated CPU {estimated_cpu_pct:.1f}% exceeds hard limit {self.cfg.cpu_hard_limit_pct:.1f}%"
                )
            if estimated_memory_pct >= self.cfg.memory_hard_limit_pct:
                ok = False
                reasons.append(
                    f"estimated memory {estimated_memory_pct:.1f}% exceeds hard limit {self.cfg.memory_hard_limit_pct:.1f}%"
                )
            if estimated_cpu_pct >= self.cfg.cpu_target_limit_pct:
                reasons.append(
                    f"estimated CPU {estimated_cpu_pct:.1f}% above target {self.cfg.cpu_target_limit_pct:.1f}%"
                )
            if estimated_memory_pct >= self.cfg.memory_target_limit_pct:
                reasons.append(
                    f"estimated memory {estimated_memory_pct:.1f}% above target {self.cfg.memory_target_limit_pct:.1f}%"
                )

            if not ok:
                rejected[node.name] = reasons
                continue

            scores.append(
                NodeScore(
                    node=node,
                    score=self._score_node(
                        node,
                        identity.app_id,
                        requested_node_type,
                        estimated_cpu_pct,
                        estimated_memory_pct,
                        min_app_count,
                        max_app_count,
                    ),
                    estimated_cpu_pct=estimated_cpu_pct,
                    estimated_memory_pct=estimated_memory_pct,
                    reasons=reasons,
                )
            )

        scores.sort(key=lambda item: item.score, reverse=True)
        return scores, rejected

    def _estimated_request(
        self,
        profile_key: tuple,
        request: ResourceRequest,
        limit: ResourceRequest,
        snapshot: ClusterSnapshot,
    ) -> ResourceRequest:
        profile = snapshot.profiles.get(profile_key)
        if not profile and self.cfg.use_generic_profile_for_new_apps:
            workload, _app_id, role = profile_key
            profile = snapshot.profiles.get((workload, "", role))

        limit_cpu = limit.cpu_cores * self.cfg.cpu_limit_estimate_ratio
        limit_memory = int(limit.memory_bytes * self.cfg.memory_limit_estimate_ratio)
        fallback = ResourceRequest(
            cpu_cores=max(request.cpu_cores, limit_cpu),
            memory_bytes=max(request.memory_bytes, limit_memory),
        )

        if not profile:
            return fallback
        return ResourceRequest(
            cpu_cores=profile.estimated_cpu(fallback.cpu_cores),
            memory_bytes=profile.estimated_memory(fallback.memory_bytes),
        )

    def _estimated_cpu_pct(self, node: NodeInfo, request: ResourceRequest) -> float:
        if node.allocatable_cpu_cores <= 0:
            return 100.0
        addition = 100.0 * request.cpu_cores / node.allocatable_cpu_cores
        return node.effective_cpu_pct() + addition

    def _estimated_memory_pct(self, node: NodeInfo, request: ResourceRequest) -> float:
        if node.allocatable_memory_bytes <= 0:
            return 100.0
        addition = 100.0 * request.memory_bytes / node.allocatable_memory_bytes
        return node.effective_memory_pct() + addition

    def _score_node(
        self,
        node: NodeInfo,
        app_id: str,
        requested_node_type: str,
        estimated_cpu_pct: float,
        estimated_memory_pct: float,
        min_app_count: int,
        max_app_count: int,
    ) -> float:
        weights = self.cfg.weights

        cpu_idle = 100.0 - clamp(estimated_cpu_pct, 0.0, 100.0)
        memory_idle = 100.0 - clamp(estimated_memory_pct, 0.0, 100.0)
        app_spread = self._app_spread_score(node, app_id, min_app_count, max_app_count)
        request_headroom = self._request_headroom_score(node)
        stability = self._stability_score(estimated_cpu_pct, estimated_memory_pct)
        node_type = 100.0 if not requested_node_type or requested_node_type == node.node_type else 0.0

        return (
            weights.cpu_idle * cpu_idle
            + weights.memory_idle * memory_idle
            + weights.app_spread * app_spread
            + weights.request_headroom * request_headroom
            + weights.stability * stability
            + weights.node_type * node_type
        )

    def _app_spread_score(
        self, node: NodeInfo, app_id: str, min_app_count: int, max_app_count: int
    ) -> float:
        if not app_id:
            return 50.0
        count = node.app_counts.get(app_id, 0)
        if max_app_count == min_app_count:
            return 100.0
        return 100.0 * (max_app_count - count) / (max_app_count - min_app_count)

    def _request_headroom_score(self, node: NodeInfo) -> float:
        cpu = 100.0 - clamp(node.request_cpu_pct(), 0.0, 100.0)
        memory = 100.0 - clamp(node.request_memory_pct(), 0.0, 100.0)
        return (cpu + memory) / 2.0

    def _stability_score(self, estimated_cpu_pct: float, estimated_memory_pct: float) -> float:
        cpu_margin = 100.0 - clamp(estimated_cpu_pct, 0.0, 100.0)
        memory_margin = 100.0 - clamp(estimated_memory_pct, 0.0, 100.0)
        return min(cpu_margin, memory_margin)
