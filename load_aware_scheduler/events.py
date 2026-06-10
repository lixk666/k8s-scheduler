import logging
from datetime import datetime, timezone

from kubernetes import client
from kubernetes.client import ApiException

LOGGER = logging.getLogger(__name__)


class EventRecorder:
    def __init__(self, core_api: client.CoreV1Api, component: str):
        self.core_api = core_api
        self.component = component

    def normal(self, pod: object, reason: str, message: str) -> None:
        self._record(pod, "Normal", reason, message)

    def warning(self, pod: object, reason: str, message: str) -> None:
        self._record(pod, "Warning", reason, message)

    def _record(self, pod: object, event_type: str, reason: str, message: str) -> None:
        try:
            now = datetime.now(timezone.utc)
            event_cls = getattr(client, "V1Event", None) or getattr(
                client, "CoreV1Event", None
            )
            if event_cls is None:
                LOGGER.debug("Kubernetes client has no core Event model")
                return

            event = event_cls(
                metadata=client.V1ObjectMeta(
                    generate_name=f"{pod.metadata.name}.",
                    namespace=pod.metadata.namespace,
                ),
                involved_object=client.V1ObjectReference(
                    api_version="v1",
                    kind="Pod",
                    namespace=pod.metadata.namespace,
                    name=pod.metadata.name,
                    uid=pod.metadata.uid,
                    resource_version=pod.metadata.resource_version,
                ),
                type=event_type,
                reason=reason,
                message=message[:1024],
                source=client.V1EventSource(component=self.component),
                first_timestamp=now,
                last_timestamp=now,
                count=1,
            )
            self.core_api.create_namespaced_event(pod.metadata.namespace, event)
        except ApiException as exc:
            LOGGER.debug("failed to create event for %s/%s: %s", pod.metadata.namespace, pod.metadata.name, exc)
        except Exception:
            LOGGER.debug(
                "failed to build event for %s/%s",
                pod.metadata.namespace,
                pod.metadata.name,
                exc_info=True,
            )
