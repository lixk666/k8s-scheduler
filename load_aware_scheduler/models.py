from dataclasses import dataclass, field
from statistics import mean
from typing import Dict, List, Optional, Tuple


PodKey = Tuple[str, str]
ProfileKey = Tuple[str, str, str]


@dataclass
class ResourceRequest:
    cpu_cores: float = 0.0
    memory_bytes: int = 0


@dataclass
class PodIdentity:
    namespace: str
    name: str
    workload: str
    app_id: str
    role: str

    @property
    def profile_key(self) -> ProfileKey:
        return (self.workload, self.app_id, self.role)


@dataclass
class PodActualUsage:
    cpu_cores: Optional[float] = None
    memory_bytes: Optional[int] = None


@dataclass
class ResourceProfile:
    cpu_samples: List[float] = field(default_factory=list)
    memory_samples: List[int] = field(default_factory=list)

    def estimated_cpu(self, fallback: float) -> float:
        if not self.cpu_samples:
            return fallback
        return max(fallback, percentile(self.cpu_samples, 0.75))

    def estimated_memory(self, fallback: int) -> int:
        if not self.memory_samples:
            return fallback
        return max(fallback, int(percentile(self.memory_samples, 0.75)))


@dataclass
class NodeInfo:
    name: str
    labels: Dict[str, str]
    taints: List[object]
    ready: bool
    unschedulable: bool
    allocatable_cpu_cores: float
    allocatable_memory_bytes: int
    requested_cpu_cores: float = 0.0
    requested_memory_bytes: int = 0
    actual_cpu_pct: Optional[float] = None
    actual_memory_pct: Optional[float] = None
    app_counts: Dict[str, int] = field(default_factory=dict)
    workload_role_counts: Dict[Tuple[str, str], int] = field(default_factory=dict)
    node_type: str = ""

    def request_cpu_pct(self) -> float:
        if self.allocatable_cpu_cores <= 0:
            return 100.0
        return 100.0 * self.requested_cpu_cores / self.allocatable_cpu_cores

    def request_memory_pct(self) -> float:
        if self.allocatable_memory_bytes <= 0:
            return 100.0
        return 100.0 * self.requested_memory_bytes / self.allocatable_memory_bytes

    def effective_cpu_pct(self) -> float:
        return self.actual_cpu_pct if self.actual_cpu_pct is not None else self.request_cpu_pct()

    def effective_memory_pct(self) -> float:
        if self.actual_memory_pct is not None:
            return self.actual_memory_pct
        return self.request_memory_pct()

    def assume(
        self,
        app_id: str,
        workload: str,
        role: str,
        cpu_cores: float,
        memory_bytes: int,
    ) -> None:
        self.requested_cpu_cores += cpu_cores
        self.requested_memory_bytes += memory_bytes
        if app_id:
            self.app_counts[app_id] = self.app_counts.get(app_id, 0) + 1
        if workload or role:
            key = (workload, role)
            self.workload_role_counts[key] = self.workload_role_counts.get(key, 0) + 1


@dataclass
class MetricsSnapshot:
    node_cpu_pct: Dict[str, float] = field(default_factory=dict)
    node_memory_pct: Dict[str, float] = field(default_factory=dict)
    pod_cpu_cores: Dict[PodKey, float] = field(default_factory=dict)
    pod_memory_bytes: Dict[PodKey, int] = field(default_factory=dict)


@dataclass
class ClusterSnapshot:
    nodes: Dict[str, NodeInfo]
    profiles: Dict[ProfileKey, ResourceProfile]
    metrics: MetricsSnapshot

    def assume(self, node_name: str, identity: PodIdentity, request: ResourceRequest) -> None:
        node = self.nodes.get(node_name)
        if node:
            node.assume(
                identity.app_id,
                identity.workload,
                identity.role,
                request.cpu_cores,
                request.memory_bytes,
            )


@dataclass
class NodeScore:
    node: NodeInfo
    score: float
    estimated_cpu_pct: float
    estimated_memory_pct: float
    reasons: List[str]


def percentile(values: List[float], fraction: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    ordered = sorted(values)
    index = int(round((len(ordered) - 1) * fraction))
    return ordered[index]


def average_or_none(values: List[float]) -> Optional[float]:
    if not values:
        return None
    return mean(values)
