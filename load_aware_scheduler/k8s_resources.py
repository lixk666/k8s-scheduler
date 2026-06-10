from typing import Dict, Iterable, List, Optional

from load_aware_scheduler.config import SchedulerConfig
from load_aware_scheduler.models import PodActualUsage, PodIdentity, ResourceRequest
from load_aware_scheduler.quantity import parse_cpu_cores, parse_memory_bytes


def pod_effective_request(pod: object) -> ResourceRequest:
    return pod_effective_resource(pod, "requests")


def pod_effective_limit(pod: object) -> ResourceRequest:
    return pod_effective_resource(pod, "limits")


def pod_effective_resource(pod: object, resource_field: str) -> ResourceRequest:
    app_cpu = 0.0
    app_memory = 0
    for container in pod.spec.containers or []:
        req = container_resource(container, resource_field)
        app_cpu += req.cpu_cores
        app_memory += req.memory_bytes

    init_cpu = 0.0
    init_memory = 0
    for container in pod.spec.init_containers or []:
        req = container_resource(container, resource_field)
        init_cpu = max(init_cpu, req.cpu_cores)
        init_memory = max(init_memory, req.memory_bytes)

    return ResourceRequest(
        cpu_cores=max(app_cpu, init_cpu),
        memory_bytes=max(app_memory, init_memory),
    )


def container_request(container: object) -> ResourceRequest:
    return container_resource(container, "requests")


def container_resource(container: object, resource_field: str) -> ResourceRequest:
    values = {}
    resources = getattr(container, "resources", None)
    if resources:
        values = getattr(resources, resource_field, None) or {}
    return ResourceRequest(
        cpu_cores=parse_cpu_cores(values.get("cpu")),
        memory_bytes=parse_memory_bytes(values.get("memory")),
    )


def pod_identity(pod: object, cfg: SchedulerConfig) -> PodIdentity:
    labels = pod.metadata.labels or {}
    role = first_label(labels, cfg.role_label_keys)
    workload = first_label(labels, cfg.workload_label_keys)
    app_id = first_label(labels, cfg.app_label_keys)

    if not workload:
        workload = infer_workload(labels, role)
    if not role:
        role = "unknown"
    if not app_id:
        app_id = pod.metadata.owner_references[0].name if pod.metadata.owner_references else pod.metadata.name

    return PodIdentity(
        namespace=pod.metadata.namespace,
        name=pod.metadata.name,
        workload=workload or "unknown",
        app_id=app_id,
        role=role,
    )


def first_label(labels: Dict[str, str], keys: Iterable[str]) -> str:
    for key in keys:
        value = labels.get(key)
        if value:
            return value
    return ""


def infer_workload(labels: Dict[str, str], role: str) -> str:
    if "spark-role" in labels or role in {"driver", "executor"}:
        return "spark"
    component = (labels.get("component") or labels.get("app.kubernetes.io/component") or "").lower()
    if component in {"taskmanager", "jobmanager"}:
        return "flink"
    if "flinkdeployment.flink.apache.org/name" in labels:
        return "flink"
    return ""


def node_type(labels: Dict[str, str], cfg: SchedulerConfig) -> str:
    return first_label(labels, cfg.node_type_label_keys)


def pod_requested_node_type(pod: object, cfg: SchedulerConfig) -> str:
    annotations = pod.metadata.annotations or {}
    labels = pod.metadata.labels or {}
    for key in cfg.node_type_label_keys:
        value = annotations.get(key) or labels.get(key)
        if value:
            return value
    return annotations.get("scheduler.load-aware/node-type", "")


def is_pod_terminal(pod: object) -> bool:
    return pod.status.phase in {"Succeeded", "Failed"}


def is_pod_pending_for_scheduler(pod: object, scheduler_name: str) -> bool:
    if pod.metadata.deletion_timestamp is not None:
        return False
    if pod.spec.node_name:
        return False
    if pod.status.phase != "Pending":
        return False
    return pod.spec.scheduler_name == scheduler_name


def pod_actual_usage(pod: object, pod_cpu: Dict[tuple, float], pod_memory: Dict[tuple, int]) -> PodActualUsage:
    key = (pod.metadata.namespace, pod.metadata.name)
    return PodActualUsage(
        cpu_cores=pod_cpu.get(key),
        memory_bytes=pod_memory.get(key),
    )


def unsupported_constraints(pod: object) -> List[str]:
    reasons: List[str] = []

    affinity = pod.spec.affinity
    if affinity:
        pod_affinity = affinity.pod_affinity
        if pod_affinity and pod_affinity.required_during_scheduling_ignored_during_execution:
            reasons.append("required podAffinity")
        pod_anti_affinity = affinity.pod_anti_affinity
        if pod_anti_affinity and pod_anti_affinity.required_during_scheduling_ignored_during_execution:
            reasons.append("required podAntiAffinity")

    for constraint in pod.spec.topology_spread_constraints or []:
        if constraint.when_unsatisfiable == "DoNotSchedule":
            reasons.append("DoNotSchedule topologySpreadConstraints")
            break

    for container in pod.spec.containers or []:
        for port in container.ports or []:
            if port.host_port:
                reasons.append("hostPort")
                break

    for volume in pod.spec.volumes or []:
        if volume.persistent_volume_claim:
            reasons.append("PersistentVolumeClaim")
            break

    return reasons
