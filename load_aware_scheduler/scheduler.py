import logging
import time
from collections import Counter
from threading import Event
from typing import Dict, List, Tuple

from kubernetes import client
from kubernetes.client import ApiException

from load_aware_scheduler.cache import ClusterCache
from load_aware_scheduler.config import SchedulerConfig
from load_aware_scheduler.dashboard import DashboardServer, ScheduleHistory
from load_aware_scheduler.events import EventRecorder
from load_aware_scheduler.k8s_resources import (
    is_pod_pending_for_scheduler,
    pod_effective_request,
    pod_identity,
    unsupported_constraints,
)
from load_aware_scheduler.models import NodeScore
from load_aware_scheduler.scoring import LoadAwareScorer

LOGGER = logging.getLogger(__name__)


class LoadAwareScheduler:
    def __init__(self, cfg: SchedulerConfig, api_client: client.ApiClient, stop_event: Event):
        self.cfg = cfg
        self.core_api = client.CoreV1Api(api_client)
        self.custom_api = client.CustomObjectsApi(api_client)
        self.cache = ClusterCache(cfg, self.core_api, self.custom_api, stop_event)
        self.scorer = LoadAwareScorer(cfg)
        self.events = EventRecorder(self.core_api, cfg.scheduler_name)
        self.schedule_history = ScheduleHistory(cfg.dashboard_history_size)
        self.dashboard = DashboardServer(cfg, self.cache, self.schedule_history, stop_event)
        self.stop_event = stop_event
        self._last_failure_event: Dict[Tuple[str, str], float] = {}

    def run_forever(self) -> None:
        LOGGER.info(
            "starting scheduler name=%s namespace=%s dry_run=%s",
            self.cfg.scheduler_name,
            self.cfg.namespace or "*",
            self.cfg.dry_run,
        )
        self.cache.start()
        self.dashboard.start()
        while not self.stop_event.is_set():
            try:
                self.schedule_once()
            except Exception:
                LOGGER.exception("schedule loop failed")
            self.stop_event.wait(self.cfg.poll_interval_seconds)

    def schedule_once(self) -> None:
        pods = self._list_pending_pods()
        if not pods:
            return

        LOGGER.info("found %s pending pods for scheduler %s", len(pods), self.cfg.scheduler_name)
        pods.sort(
            key=lambda pod: (
                pod.metadata.creation_timestamp.timestamp()
                if pod.metadata.creation_timestamp
                else 0
            )
        )
        for pod in pods:
            self._schedule_pod(pod)

    def _schedule_pod(self, pod: object) -> None:
        if not is_pod_pending_for_scheduler(pod, self.cfg.scheduler_name):
            return

        unsupported = unsupported_constraints(pod)
        if unsupported and self.cfg.strict_unsupported_constraints:
            message = "unsupported hard scheduling constraints: " + ", ".join(unsupported)
            self._record_failure(pod, "UnsupportedConstraints", message)
            LOGGER.warning("skip %s/%s: %s", pod.metadata.namespace, pod.metadata.name, message)
            return

        snapshot = self.cache.snapshot()
        scores, rejected = self.scorer.score_pod(pod, snapshot)
        if not scores:
            message = self._summarize_rejections(rejected)
            self._record_failure(pod, "FailedScheduling", message)
            LOGGER.warning("no fit for %s/%s: %s", pod.metadata.namespace, pod.metadata.name, message)
            return

        self._log_node_scores(pod, scores, rejected)
        best = scores[0]
        if self.cfg.dry_run:
            self.schedule_history.record(
                pod,
                scores,
                rejected,
                best.node.name,
                "dry-run",
                self.cfg,
            )
            LOGGER.info(
                "dry-run schedule %s/%s -> %s score=%.2f cpu=%.1f%% memory=%.1f%%",
                pod.metadata.namespace,
                pod.metadata.name,
                best.node.name,
                best.score,
                best.estimated_cpu_pct,
                best.estimated_memory_pct,
            )
            return

        status = "scheduled" if self._bind(pod, best) else "binding-failed"
        self.schedule_history.record(
            pod,
            scores,
            rejected,
            best.node.name,
            status,
            self.cfg,
        )

    def _bind(self, pod: object, score: NodeScore) -> bool:
        target = client.V1ObjectReference(api_version="v1", kind="Node", name=score.node.name)
        body = client.V1Binding(
            metadata=client.V1ObjectMeta(name=pod.metadata.name, namespace=pod.metadata.namespace),
            target=target,
        )

        try:
            self.core_api.create_namespaced_pod_binding(
                pod.metadata.name,
                pod.metadata.namespace,
                body,
                _preload_content=False,
            )
            self.cache.assume(score.node.name, pod)
            identity = pod_identity(pod, self.cfg)
            request = pod_effective_request(pod)
            message = (
                f"assigned by {self.cfg.scheduler_name} to {score.node.name}; "
                f"score={score.score:.2f}; estimated_cpu={score.estimated_cpu_pct:.1f}%; "
                f"estimated_memory={score.estimated_memory_pct:.1f}%; "
                f"app={identity.app_id}; role={identity.role}; "
                f"request_cpu={request.cpu_cores:.3f}; request_memory={request.memory_bytes}"
            )
            self.events.normal(pod, "Scheduled", message)
            LOGGER.info("scheduled %s/%s -> %s", pod.metadata.namespace, pod.metadata.name, score.node.name)
            return True
        except ApiException as exc:
            if exc.status == 409:
                LOGGER.info("pod %s/%s was already scheduled", pod.metadata.namespace, pod.metadata.name)
                return True
            self._record_failure(pod, "BindingFailed", str(exc))
            LOGGER.warning("failed to bind %s/%s: %s", pod.metadata.namespace, pod.metadata.name, exc)
            return False

    def _list_pending_pods(self) -> List[object]:
        field_selector = "status.phase=Pending"
        if self.cfg.namespace:
            items = self.core_api.list_namespaced_pod(
                self.cfg.namespace, field_selector=field_selector
            ).items
        else:
            items = self.core_api.list_pod_for_all_namespaces(
                field_selector=field_selector
            ).items
        return [
            pod
            for pod in items
            if is_pod_pending_for_scheduler(pod, self.cfg.scheduler_name)
        ]

    def _record_failure(self, pod: object, reason: str, message: str) -> None:
        key = (pod.metadata.namespace, pod.metadata.name)
        now = time.time()
        last = self._last_failure_event.get(key, 0)
        if now - last >= self.cfg.failure_event_interval_seconds:
            self.events.warning(pod, reason, message)
            self._last_failure_event[key] = now

    def _log_node_scores(
        self,
        pod: object,
        scores: List[NodeScore],
        rejected: Dict[str, List[str]],
    ) -> None:
        if not self.cfg.log_node_scores:
            return

        limit = self.cfg.log_node_score_limit
        visible_scores = scores[:limit] if limit > 0 else scores
        score_lines = []
        for rank, item in enumerate(visible_scores, start=1):
            node = item.node
            score_lines.append(
                (
                    "#{rank} {node} score={score:.2f} "
                    "estimated_cpu={estimated_cpu:.1f}% "
                    "estimated_memory={estimated_memory:.1f}% "
                    "actual_cpu={actual_cpu} actual_memory={actual_memory} "
                    "request_cpu={request_cpu:.1f}% "
                    "request_memory={request_memory:.1f}% "
                    "app_count={app_count}"
                ).format(
                    rank=rank,
                    node=node.name,
                    score=item.score,
                    estimated_cpu=item.estimated_cpu_pct,
                    estimated_memory=item.estimated_memory_pct,
                    actual_cpu=self._format_pct(node.actual_cpu_pct),
                    actual_memory=self._format_pct(node.actual_memory_pct),
                    request_cpu=node.request_cpu_pct(),
                    request_memory=node.request_memory_pct(),
                    app_count=node.app_counts.get(
                        pod_identity(pod, self.cfg).app_id,
                        0,
                    ),
                )
            )

        suffix = ""
        if limit > 0 and len(scores) > limit:
            suffix = f"; hidden_candidates={len(scores) - limit}"

        LOGGER.info(
            "node scores for %s/%s: %s%s",
            pod.metadata.namespace,
            pod.metadata.name,
            " | ".join(score_lines),
            suffix,
        )

        if self.cfg.log_rejected_nodes and rejected:
            rejected_lines = [
                f"{node}: {', '.join(reasons)}"
                for node, reasons in sorted(rejected.items())
            ]
            LOGGER.info(
                "rejected nodes for %s/%s: %s",
                pod.metadata.namespace,
                pod.metadata.name,
                " | ".join(rejected_lines),
            )

    def _format_pct(self, value: object) -> str:
        if value is None:
            return "unknown"
        return f"{float(value):.1f}%"

    def _summarize_rejections(self, rejected: Dict[str, List[str]]) -> str:
        if not rejected:
            return "no nodes available"

        counts = Counter()
        for reasons in rejected.values():
            for reason in reasons:
                counts[reason] += 1

        top = ", ".join(f"{reason} ({count})" for reason, count in counts.most_common(5))
        return f"0/{len(rejected)} nodes are available: {top}"
