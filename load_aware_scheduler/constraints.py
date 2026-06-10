from typing import Dict, Iterable, List, Tuple

from load_aware_scheduler.models import NodeInfo, ResourceRequest


def node_matches_required_constraints(
    pod: object,
    node: NodeInfo,
    request: ResourceRequest,
    request_fit_ratio: float,
) -> Tuple[bool, List[str]]:
    reasons: List[str] = []

    if not node.ready:
        reasons.append("node is not Ready")
    if node.unschedulable:
        reasons.append("node is unschedulable")
    if not matches_node_selector(pod, node):
        reasons.append("nodeSelector mismatch")
    if not matches_required_node_affinity(pod, node):
        reasons.append("required nodeAffinity mismatch")
    if not tolerates_node_taints(pod, node):
        reasons.append("untolerated NoSchedule taint")
    if not request_fits(node, request, request_fit_ratio):
        reasons.append("insufficient requested resource headroom")

    return len(reasons) == 0, reasons


def matches_node_selector(pod: object, node: NodeInfo) -> bool:
    selector = pod.spec.node_selector or {}
    for key, value in selector.items():
        if node.labels.get(key) != value:
            return False
    return True


def matches_required_node_affinity(pod: object, node: NodeInfo) -> bool:
    affinity = pod.spec.affinity
    if not affinity or not affinity.node_affinity:
        return True

    required = affinity.node_affinity.required_during_scheduling_ignored_during_execution
    if not required or not required.node_selector_terms:
        return True

    return any(node_selector_term_matches(term, node) for term in required.node_selector_terms)


def node_selector_term_matches(term: object, node: NodeInfo) -> bool:
    expressions = term.match_expressions or []
    fields = term.match_fields or []
    return all(expression_matches(node.labels, expr) for expr in expressions) and all(
        field_matches(node, field) for field in fields
    )


def expression_matches(labels: Dict[str, str], expr: object) -> bool:
    key = expr.key
    operator = expr.operator
    values = expr.values or []
    current = labels.get(key)

    if operator == "In":
        return current in values
    if operator == "NotIn":
        return current is None or current not in values
    if operator == "Exists":
        return current is not None
    if operator == "DoesNotExist":
        return current is None
    if operator == "Gt":
        try:
            return current is not None and int(current) > int(values[0])
        except (TypeError, ValueError, IndexError):
            return False
    if operator == "Lt":
        try:
            return current is not None and int(current) < int(values[0])
        except (TypeError, ValueError, IndexError):
            return False
    return False


def field_matches(node: NodeInfo, field: object) -> bool:
    key = field.key
    operator = field.operator
    values = field.values or []
    if key != "metadata.name":
        return False

    if operator == "In":
        return node.name in values
    if operator == "NotIn":
        return node.name not in values
    return False


def tolerates_node_taints(pod: object, node: NodeInfo) -> bool:
    tolerations = pod.spec.tolerations or []
    for taint in node.taints or []:
        if taint.effect not in {"NoSchedule", "NoExecute"}:
            continue
        if not any(toleration_matches(toleration, taint) for toleration in tolerations):
            return False
    return True


def toleration_matches(toleration: object, taint: object) -> bool:
    if toleration.effect and toleration.effect != taint.effect:
        return False

    operator = toleration.operator or "Equal"
    if operator == "Exists":
        return not toleration.key or toleration.key == taint.key

    return toleration.key == taint.key and (toleration.value or "") == (taint.value or "")


def request_fits(node: NodeInfo, request: ResourceRequest, request_fit_ratio: float) -> bool:
    cpu_limit = node.allocatable_cpu_cores * request_fit_ratio
    memory_limit = node.allocatable_memory_bytes * request_fit_ratio
    return (
        node.requested_cpu_cores + request.cpu_cores <= cpu_limit
        and node.requested_memory_bytes + request.memory_bytes <= memory_limit
    )
