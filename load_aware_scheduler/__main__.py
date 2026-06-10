import logging
import signal
import sys
from threading import Event

from kubernetes import client
from kubernetes import config as k8s_config
from kubernetes.config.config_exception import ConfigException

from load_aware_scheduler.config import SchedulerConfig
from load_aware_scheduler.scheduler import LoadAwareScheduler


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def load_kubernetes_config() -> None:
    try:
        k8s_config.load_incluster_config()
        logging.info("loaded in-cluster Kubernetes config")
        return
    except ConfigException:
        pass

    k8s_config.load_kube_config()
    logging.info("loaded local kubeconfig")


def main() -> int:
    configure_logging()
    load_kubernetes_config()

    stop_event = Event()

    def stop(_signum, _frame) -> None:
        logging.info("shutdown requested")
        stop_event.set()

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    cfg = SchedulerConfig.from_env()
    api_client = client.ApiClient()
    scheduler = LoadAwareScheduler(cfg, api_client, stop_event)
    scheduler.run_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main())
