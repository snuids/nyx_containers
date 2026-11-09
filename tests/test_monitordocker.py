"""The docker monitor: what `/topic/DOCKER_COMMAND` may do, and the `docker_status` it publishes.

This container is the only one that can restart or stop another container, so the command path is
worth a test of its own. `load_data` builds the documents Kibana shows: the percentage and the
icons are what the operations dashboard is read on.
"""

import json
import os
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

# The module names itself after the node it runs on at import time.
os.environ.setdefault("NODE_NAME", "node1")

from harness import FakeES, Harness                                       # noqa: E402

SCENARIO = Harness()
monitor = SCENARIO.load("nyx_monitordocker").module


class FakeContainer:
    def __init__(self, name, status="running", image="snuids/nyx_rest:v3.22.27"):
        self.name = name
        self.status = status
        self.attrs = {
            "Config": {"Image": image},
            "Created": "2026-11-01T10:00:00Z",
            "State": {"StartedAt": "2026-11-01T10:00:05Z"},
        }
        self.calls = []

    def stats(self, stream=False):
        if self.status != "running":
            raise RuntimeError("not running")
        return {
            "memory_stats": {"usage": 12000000},
            "cpu_stats": {"cpu_usage": {"total_usage": 220},
                          "system_cpu_usage": 2000},
            "precpu_stats": {"cpu_usage": {"total_usage": 100},
                             "system_cpu_usage": 1000},
        }

    def restart(self):
        self.calls.append("restart")

    def stop(self):
        self.calls.append("stop")

    def start(self):
        self.calls.append("start")


class FakeDocker:
    """Double of `docker.from_env()`: the `containers.list()` shape the monitor walks."""

    def __init__(self, containers):
        self._store = list(containers)

    @property
    def containers(self):
        return self

    def list(self, all=False):
        return self._store


class CommandTests(unittest.TestCase):
    """`/topic/DOCKER_COMMAND`: restart, stop or start one named container."""

    def setUp(self):
        self.api = FakeContainer("docker-compose-nyx_restapi-1")
        self.other = FakeContainer("grafana")
        monitor.client = FakeDocker([self.api, self.other])
        self.addCleanup(setattr, monitor, "client", None)

    def command(self, name, command):
        monitor.messageReceived("DOCKER_COMMAND", json.dumps({"name": name, "command": command}),
                                {})

    def test_a_restart_only_touches_the_named_container(self):
        self.command("docker-compose-nyx_restapi-1", "restart")
        self.assertEqual(self.api.calls, ["restart"])
        self.assertEqual(self.other.calls, [])

    def test_stop_and_start_are_dispatched(self):
        self.command("grafana", "stop")
        self.command("grafana", "start")
        self.assertEqual(self.other.calls, ["stop", "start"])

    def test_an_unknown_command_does_nothing(self):
        self.command("grafana", "remove")
        self.assertEqual(self.other.calls, [])

    def test_an_unknown_container_does_nothing(self):
        self.command("nope", "restart")
        self.assertEqual(self.api.calls + self.other.calls, [])


class LoadDataTests(unittest.TestCase):
    """The `docker_status` documents: one per container, running or not."""

    def setUp(self):
        self.es = FakeES()
        monitor.es = self.es
        self.addCleanup(setattr, monitor, "es", None)

    def publish(self, containers):
        monitor.client = FakeDocker(containers)
        self.addCleanup(setattr, monitor, "client", None)
        monitor.load_data()
        return self.es

    def test_a_running_container_carries_its_usage_and_a_green_icon(self):
        self.publish([FakeContainer("grafana")])

        document = self.es.bulk_documents()[0]
        self.assertEqual(document["status"], "running")
        self.assertEqual(document["shortname"], "grafana")
        self.assertEqual(document["node"], "node1")
        self.assertEqual(document["image"], "snuids/nyx_rest:v3.22.27")
        self.assertEqual(document["memory_used_mb"], 12)
        self.assertEqual(document["cpu_percent"], 12.0)
        self.assertEqual(document["result_icon"], "regular/check-circle>#0F9D58")

    def test_a_stopped_container_is_still_published_with_a_red_icon(self):
        self.publish([FakeContainer("nyx_ui", status="exited")])

        document = self.es.bulk_documents()[0]
        self.assertEqual(document["status"], "exited")
        self.assertNotIn("memory_used", document)
        self.assertEqual(document["result_icon"], "exclamation-circle>#DB4437")

    def test_the_document_id_is_the_container_name_without_spaces(self):
        self.publish([FakeContainer("my container")])
        self.assertEqual(self.es.bulk_actions()[0]["index"]["_id"], "mycontainer")
        self.assertEqual(self.es.bulk_actions()[0]["index"]["_index"], "docker_status")

    def test_no_docker_daemon_publishes_nothing(self):
        monitor.client = None
        monitor.load_data()
        self.assertEqual(self.es.bulk_calls, [])

    def test_missing_stats_do_not_drop_the_document(self):
        container = FakeContainer("weird")
        container.stats = lambda stream=False: {"memory_stats": {}}

        self.publish([container])

        document = self.es.bulk_documents()[0]
        self.assertEqual(document["name"], "weird")
        self.assertNotIn("cpu_percent", document)


if __name__ == "__main__":
    unittest.main()
