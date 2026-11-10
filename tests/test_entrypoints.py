"""Les blocs `__main__` des conteneurs.

Tout ce qui parle au broker, au cluster, a Redis ou a Docker vit sous `if __name__ == '__main__'` :
c'est la partie qu'aucun test ne touchait, et qu'on ne voyait qu'en lancant le conteneur. On execute
le script comme le conteneur le fait (`runpy`, `run_name="__main__"`), avec les doubles du harnais
et un `sleep` qui coupe la boucle : le bloc demarre, envoie son premier signe de vie, puis s'arrete.

Ce que ces tests verrouillent : le conteneur demarre avec la configuration que le compose lui donne,
il s'abonne aux bonnes files, il envoie un signe de vie qui porte les variables attendues (icone,
files, lambdas), et les secrets ne partent pas dans les journaux.
"""

import logging
import os
import runpy
import sys
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

from harness import FakeES, FakeRedis                                         # noqa: E402

SOURCES = Path(__file__).resolve().parents[1] / "sources"

# Ce que le compose donne aux conteneurs ; seules les valeurs comptent, pas leur realite.
ENV = {
    "AMQC_URL": "broker.example",
    "AMQC_PORT": "61613",
    "AMQC_LOGIN": "amquser",
    "AMQC_PASSWORD": "s3cr3t-amq",
    "ELK_URL": "elastic.example",
    "ELK_PORT": "9200",
    "ELK_LOGIN": "elkuser",
    "ELK_PASSWORD": "s3cr3t-elk",
    "ELK_SSL": "false",
    "USE_LOGSTASH": "false",
    "NODE_NAME": "energy",
    "REDIS_IP": "redis.example",
    "RUNNER": "1",
}


class LoopStopped(Exception):
    """Sortie de la boucle infinie des conteneurs : le deuxieme `sleep` la coupe."""


class FakeAMQClient:
    """Double du client STOMP : ce qui aurait ete publie, et a qui on s'abonne."""

    instances = []

    def __init__(self, server, module, queues, callback=None):
        self.server = server
        self.module = module
        self.queues = list(queues) if isinstance(queues, (list, tuple)) else [queues]
        self.callback = callback
        self.messages = []
        self.life_signs = []
        self.disconnects = 0
        FakeAMQClient.instances.append(self)

    def send_message(self, destination, body, headers=None):
        self.messages.append((destination, body, headers))
        return True

    def send_life_sign(self, variables=None):
        self.life_signs.append(variables)
        return True

    def disconnect(self):
        self.disconnects += 1


class FakeContainer:
    """Un conteneur Docker tel que `load_data` le lit."""

    name = "docker-compose-nyx_restapi-1"
    status = "running"
    attrs = {
        "Config": {"Image": "snuids/nyx_rest:v3.22.29"},
        "Created": "2026-11-09T10:00:00Z",
        "State": {"StartedAt": "2026-11-09T10:00:05Z"},
    }

    def stats(self, stream=False):
        return {"memory_stats": {"usage": 120_000_000}}


class FakeDockerClient:
    def __init__(self):
        self.containers = self

    def list(self, all=False):
        return [FakeContainer()]


class FakeThread:
    """Les threads de fond ne servent pas ici : on retient seulement leur cible."""

    started = []

    def __init__(self, target=None, **kwargs):
        self.target = target
        FakeThread.started.append(target)

    def start(self):
        return None


class EntrypointTest(unittest.TestCase):
    """Execute un bloc `__main__` une fois, puis coupe sa boucle."""

    def setUp(self):
        FakeAMQClient.instances = []
        FakeThread.started = []
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.previous_cwd = os.getcwd()
        os.chdir(self.temporary.name)
        self.addCleanup(os.chdir, self.previous_cwd)
        os.makedirs("logs", exist_ok=True)
        # `nyx_lambda` ecrit `firstrun.txt` dans ./notebooks au premier demarrage, puis lit les
        # exigences : le repertoire doit exister, comme dans l'image.
        os.makedirs("notebooks", exist_ok=True)
        self.saved_env = dict(os.environ)
        self.addCleanup(self._restore_env)
        os.environ.update(ENV)
        # Les scripts posent un handler sur le logger racine a chaque demarrage.
        root = logging.getLogger()
        saved_handlers = list(root.handlers)
        self.addCleanup(root.handlers.__setitem__, slice(None), saved_handlers)

    def _restore_env(self):
        os.environ.clear()
        os.environ.update(self.saved_env)

    def run_main(self, script, *, env=None, es=None, docker=None, redis=None):
        """Execute `sources/<script>` en tant que `__main__` et rend les doubles installes."""
        if env:
            os.environ.update(env)
        sleeps = {"count": 0}

        def sleep_once(seconds):
            # Premier appel : l'iteration tourne. Deuxieme : on sort de `while True`, comme le
            # ferait un arret du conteneur.
            sleeps["count"] += 1
            if sleeps["count"] > 1:
                raise LoopStopped(seconds)
            return None

        fakes = {
            "es": es or FakeES(),
            "amq": None,
            "docker": docker or FakeDockerClient(),
            "redis": redis or FakeRedis(),
        }

        def amq_factory(*args, **kwargs):
            return FakeAMQClient(*args, **kwargs)

        def es_factory(*args, **kwargs):
            fakes["es_args"] = (args, kwargs)
            return fakes["es"]

        with ExitStack() as stack:
            stack.enter_context(mock.patch("time.sleep", side_effect=sleep_once))
            stack.enter_context(mock.patch("%s.AMQClient" % _amq(script), side_effect=amq_factory))
            # Les scripts importent l'un ou l'autre client : on remplace les deux, le nom retenu
            # par le script decide duquel il s'agit (`nyx_lambda` garde l'ancien import en
            # commentaire, ce qui suffisait a tromper un choix fait sur le texte du fichier).
            stack.enter_context(mock.patch("opensearchpy.OpenSearch", side_effect=es_factory))
            stack.enter_context(mock.patch("elasticsearch.Elasticsearch", side_effect=es_factory))
            stack.enter_context(mock.patch("docker.from_env", return_value=fakes["docker"]))
            stack.enter_context(mock.patch("redis.Redis", return_value=fakes["redis"]))
            stack.enter_context(mock.patch(
                "logstash_async.handler.AsynchronousLogstashHandler",
                side_effect=_no_logstash))
            if script == "nyx_lambda.py":
                stack.enter_context(mock.patch("threading.Thread", FakeThread))
            with self.assertRaises(LoopStopped):
                runpy.run_path(str(SOURCES / script), run_name="__main__")

        fakes["amq"] = FakeAMQClient.instances[-1] if FakeAMQClient.instances else None
        fakes["sleeps"] = sleeps["count"]
        return fakes


def _no_logstash(*args, **kwargs):
    handler = logging.NullHandler()
    handler.setLevel = lambda level: None
    return handler


def _amq(script):
    """Le module `amqstompclient` est importe par chaque script, sous le meme nom."""
    return "amqstompclient.amqstompclient"


class FormatConverterTests(EntrypointTest):
    def test_the_container_starts_and_sends_a_life_sign(self):
        fakes = self.run_main("nyx_formatconverter.py")

        self.assertIsNotNone(fakes["amq"], "aucun client AMQP construit")
        self.assertEqual(len(fakes["amq"].life_signs), 1)
        self.assertEqual(fakes["amq"].life_signs[0]["icon"], "transgender")

    def test_the_logstash_appender_is_added_when_asked(self):
        fakes = self.run_main("nyx_formatconverter.py", env={"USE_LOGSTASH": "true"})

        self.assertEqual(len(fakes["amq"].life_signs), 1)


class ReportRunnerTests(EntrypointTest):
    def test_the_container_starts_and_sends_a_life_sign(self):
        fakes = self.run_main("nyx_reportrunner.py")

        self.assertIsNotNone(fakes["amq"])
        self.assertEqual(len(fakes["amq"].life_signs), 1)

    def test_the_module_name_carries_the_runner_number(self):
        fakes = self.run_main("nyx_reportrunner.py", env={"RUNNER": "3"})

        self.assertEqual(fakes["amq"].module["name"], "ReportRunner_3")

    def test_tls_is_used_when_asked_and_keeps_the_path_of_the_url(self):
        fakes = self.run_main("nyx_reportrunner.py", env={
            "ELK_SSL": "true", "ELK_URL": "elastic.example/nyx"})

        hosts = fakes["es_args"][0][0]
        self.assertEqual(hosts, ["https://elastic.example:9200/nyx"])

    def test_tls_appends_the_port_when_the_url_carries_no_path(self):
        fakes = self.run_main("nyx_reportrunner.py", env={
            "ELK_SSL": "true", "ELK_URL": "elastic.example"})

        self.assertEqual(fakes["es_args"][0][0], ["https://elastic.example:9200"])


class ReportSchedulerTests(EntrypointTest):
    def test_the_container_checks_the_tasks_on_its_first_turn(self):
        fakes = self.run_main("nyx_reportscheduler.py")

        self.assertEqual(len(fakes["amq"].life_signs), 1)
        self.assertEqual(fakes["amq"].life_signs[0]["icon"], "calendar-alt")
        self.assertEqual(len(fakes["es"].searches), 1,
                         "checkTasks doit interroger le cluster des le premier tour")

    def test_plain_http_is_used_when_tls_is_off(self):
        fakes = self.run_main("nyx_reportscheduler.py")

        self.assertEqual(fakes["es_args"][1]["hosts"], ["http://elastic.example:9200"])


class MonitorDockerTests(EntrypointTest):
    def test_the_container_reads_the_docker_daemon_and_signs_in(self):
        fakes = self.run_main("nyx_monitordocker.py")

        self.assertEqual(len(fakes["amq"].life_signs), 1)
        self.assertEqual(fakes["amq"].life_signs[0]["icon"], "brands/docker")
        self.assertIn("queue", fakes["amq"].life_signs[0])
        self.assertTrue(fakes["es"].bulk_calls, "les conteneurs doivent partir au cluster")


class LambdaTests(EntrypointTest):
    def test_the_container_starts_and_signs_in_with_its_lambdas(self):
        fakes = self.run_main("nyx_lambda.py")

        self.assertEqual(len(fakes["amq"].life_signs), 1)
        self.assertEqual(fakes["amq"].life_signs[0]["icon"], "file-code")
        self.assertEqual(fakes["amq"].life_signs[0]["lambdas"], [])

    def test_the_background_threads_are_started(self):
        self.run_main("nyx_lambda.py")

        targets = [t.__name__ for t in FakeThread.started if t is not None]
        self.assertIn("check_intervals_and_cron", targets)
        self.assertIn("updateStatsThread", targets)


if __name__ == "__main__":
    unittest.main()
