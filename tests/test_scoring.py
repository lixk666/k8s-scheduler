from types import SimpleNamespace
import unittest

from load_aware_scheduler.config import SchedulerConfig
from load_aware_scheduler.models import ClusterSnapshot, MetricsSnapshot, NodeInfo
from load_aware_scheduler.scoring import LoadAwareScorer


def ns(**kwargs):
    return SimpleNamespace(**kwargs)


class ScoringTest(unittest.TestCase):
    def test_prefers_lower_actual_cpu_node(self):
        cfg = SchedulerConfig.from_env()
        pod = ns(
            metadata=ns(
                namespace="default",
                name="spark-exec-1",
                labels={
                    "bigdata/workload": "spark",
                    "bigdata/role": "executor",
                    "bigdata/app-id": "app-1",
                },
                annotations={},
                owner_references=[],
            ),
            spec=ns(
                containers=[
                    ns(
                        resources=ns(
                            requests={
                                "cpu": "1",
                                "memory": "1Gi",
                            }
                        )
                    )
                ],
                init_containers=[],
                node_selector={},
                affinity=None,
                tolerations=[],
            ),
        )
        hot = NodeInfo(
            name="hot",
            labels={},
            taints=[],
            ready=True,
            unschedulable=False,
            allocatable_cpu_cores=10,
            allocatable_memory_bytes=10 * 1024 * 1024 * 1024,
            actual_cpu_pct=78,
            actual_memory_pct=40,
        )
        cold = NodeInfo(
            name="cold",
            labels={},
            taints=[],
            ready=True,
            unschedulable=False,
            allocatable_cpu_cores=10,
            allocatable_memory_bytes=10 * 1024 * 1024 * 1024,
            actual_cpu_pct=20,
            actual_memory_pct=40,
        )
        snapshot = ClusterSnapshot(
            nodes={"hot": hot, "cold": cold},
            profiles={},
            metrics=MetricsSnapshot(),
        )

        scores, _ = LoadAwareScorer(cfg).score_pod(pod, snapshot)

        self.assertEqual(scores[0].node.name, "cold")

    def test_uses_limit_ratio_when_profile_is_missing(self):
        cfg = SchedulerConfig.from_env()
        pod = ns(
            metadata=ns(
                namespace="default",
                name="flink-tm-1",
                labels={
                    "bigdata/workload": "flink",
                    "bigdata/role": "taskmanager",
                    "bigdata/app-id": "new-app",
                },
                annotations={},
                owner_references=[],
            ),
            spec=ns(
                containers=[
                    ns(
                        resources=ns(
                            requests={
                                "cpu": "100m",
                                "memory": "512Mi",
                            },
                            limits={
                                "cpu": "2",
                                "memory": "4Gi",
                            },
                        )
                    )
                ],
                init_containers=[],
                node_selector={},
                affinity=None,
                tolerations=[],
            ),
        )
        node = NodeInfo(
            name="node-a",
            labels={},
            taints=[],
            ready=True,
            unschedulable=False,
            allocatable_cpu_cores=10,
            allocatable_memory_bytes=10 * 1024 * 1024 * 1024,
            actual_cpu_pct=0,
            actual_memory_pct=0,
        )
        snapshot = ClusterSnapshot(
            nodes={"node-a": node},
            profiles={},
            metrics=MetricsSnapshot(),
        )

        scores, _ = LoadAwareScorer(cfg).score_pod(pod, snapshot)

        self.assertAlmostEqual(scores[0].estimated_cpu_pct, 10.0)
        self.assertAlmostEqual(scores[0].estimated_memory_pct, 28.0, places=1)

    def test_new_app_falls_back_to_workload_role_spread(self):
        cfg = SchedulerConfig.from_env()
        pod = ns(
            metadata=ns(
                namespace="default",
                name="flink-tm-new",
                labels={
                    "bigdata/workload": "flink",
                    "bigdata/role": "taskmanager",
                    "bigdata/app-id": "brand-new-app",
                },
                annotations={},
                owner_references=[],
            ),
            spec=ns(
                containers=[
                    ns(
                        resources=ns(
                            requests={
                                "cpu": "1",
                                "memory": "1Gi",
                            },
                            limits={
                                "cpu": "2",
                                "memory": "2Gi",
                            },
                        )
                    )
                ],
                init_containers=[],
                node_selector={},
                affinity=None,
                tolerations=[],
            ),
        )
        crowded = NodeInfo(
            name="crowded",
            labels={},
            taints=[],
            ready=True,
            unschedulable=False,
            allocatable_cpu_cores=10,
            allocatable_memory_bytes=10 * 1024 * 1024 * 1024,
            actual_cpu_pct=20,
            actual_memory_pct=20,
            workload_role_counts={("flink", "taskmanager"): 10},
        )
        sparse = NodeInfo(
            name="sparse",
            labels={},
            taints=[],
            ready=True,
            unschedulable=False,
            allocatable_cpu_cores=10,
            allocatable_memory_bytes=10 * 1024 * 1024 * 1024,
            actual_cpu_pct=20,
            actual_memory_pct=20,
            workload_role_counts={("flink", "taskmanager"): 1},
        )
        snapshot = ClusterSnapshot(
            nodes={"crowded": crowded, "sparse": sparse},
            profiles={},
            metrics=MetricsSnapshot(),
        )

        scores, _ = LoadAwareScorer(cfg).score_pod(pod, snapshot)

        self.assertEqual(scores[0].node.name, "sparse")


if __name__ == "__main__":
    unittest.main()
