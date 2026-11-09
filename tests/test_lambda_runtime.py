"""The lambda runtime: what it loads from the notebooks, and how it dispatches a message.

`loadConfig` turns a directory of `.ipynb` files into the lambdas the runner can call, publishes
their definitions in `nyx_lambda` and restores each next run from Redis. `messageReceived` is what
a lambda is called through — on a topic, on a REST path, or on the command topic — and `save_log`
is what keeps a trace of each run. None of it had a test: a lambda that stopped being loaded, or
one whose crash stopped being logged, was invisible.
"""

import datetime
import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

from harness import FakeBroker, FakeES, FakeRedis, Harness                # noqa: E402

SCENARIO = Harness()
lambdas = SCENARIO.load("nyx_lambda").module

RUNNER = "runner1"


def notebook(path, *cells):
    """Writes a minimal .ipynb whose code cells are the strings given."""
    document = {"metadata": {}, "nbformat": 4, "nbformat_minor": 2,
                "cells": [{"cell_type": "code", "metadata": {}, "outputs": [],
                           "execution_count": None,
                           "source": cell.splitlines(keepends=True)} for cell in cells]}
    with open(path, "w") as handle:
        json.dump(document, handle)


class LambdaTestCase(unittest.TestCase):
    """Installs the doubles every runtime function reaches for."""

    def setUp(self):
        self.environment = mock.patch.dict(os.environ, {"RUNNER": RUNNER})
        self.environment.start()
        self.addCleanup(self.environment.stop)

        self.es = FakeES()
        self.broker = FakeBroker()
        self.redis = FakeRedis()
        self.saved = {}
        for name, value in (("es", self.es), ("conn", self.broker), ("redisserver", self.redis),
                            ("elkversion", 8), ("lambdas", []), ("lambdasht", {}),
                            ("lambdaschanged", {}), ("logs", []), ("global_icon", "question")):
            self.saved[name] = getattr(lambdas, name, None)
            setattr(lambdas, name, value)

        self.directory = os.path.realpath(tempfile.mkdtemp())
        self.saved_path = lambdas.path
        lambdas.path = self.directory
        self.addCleanup(self.restore)

    def restore(self):
        for name, value in self.saved.items():
            setattr(lambdas, name, value)
        lambdas.path = self.saved_path
        lambdas.thread_check_intervals = None
        shutil.rmtree(self.directory, ignore_errors=True)


class LoadConfigTests(LambdaTestCase):
    """A directory of notebooks becomes callable lambdas, mapped to their trigger."""

    def write_notebook(self, name, *cells):
        notebook(os.path.join(self.directory, name), *cells)

    def loaded(self):
        lambdas.loadConfig()
        return {lamb["orgfunction"]: lamb for lamb in lambdas.lambdas}

    def test_a_message_lambda_is_loaded_under_the_notebook_namespace(self):
        self.write_notebook("orders.ipynb",
                            "#@LISTEN=queue/ORDERS\n\ndef on_orders():\n    return 'ok'\n")

        found = self.loaded()["on_orders"]

        self.assertEqual(found["topics"], ["queue/ORDERS"])
        self.assertIn("ordersipynb", found["function"], "the notebook name prefixes the function")
        self.assertEqual(found["code"](), "ok")
        self.assertEqual(found["runner"], RUNNER)

    def test_two_notebooks_defining_the_same_name_do_not_collide(self):
        self.write_notebook("a.ipynb", "#@LISTEN=queue/A\n\ndef run():\n    return 'a'\n")
        self.write_notebook("b.ipynb", "#@LISTEN=queue/B\n\ndef run():\n    return 'b'\n")

        lambdas.loadConfig()
        codes = sorted(lamb["code"]() for lamb in lambdas.lambdas)

        self.assertEqual(codes, ["a", "b"], "each notebook keeps its own function")

    def test_the_common_block_is_available_to_every_cell_of_its_notebook(self):
        self.write_notebook("common.ipynb",
                            "#@COMMON\nSHARED = 'from-common'\n",
                            "#@LISTEN=queue/C\n\ndef on_common():\n    return SHARED\n")

        self.assertEqual(self.loaded()["on_common"]["code"](), "from-common")

    def test_an_interval_and_a_cron_are_decoded(self):
        self.write_notebook("timers.ipynb",
                            "#@INTERVAL=5m\n\ndef every_five():\n    return 1\n",
                            "#@CRON=*/5 * * * *\n\ndef on_cron():\n    return 2\n")

        found = self.loaded()

        self.assertEqual(found["every_five"]["interval"], 300)
        self.assertIsNotNone(found["on_cron"]["crontab"])

    def test_an_undecodable_interval_skips_the_cell(self):
        self.write_notebook("bad.ipynb", "#@INTERVAL=5 bananas\n\ndef broken():\n    return 1\n")

        self.assertEqual(self.loaded(), {})

    def test_a_restapi_lambda_is_mapped_to_its_path(self):
        self.write_notebook("api.ipynb", "#@RESTAPI=calc\n\ndef on_api():\n    return 3\n")

        self.loaded()

        self.assertEqual(len(lambdas.lambdasht["/restapi/calc"]), 1)

    def test_a_cell_that_does_not_compile_is_kept_with_its_traceback(self):
        self.write_notebook("broken.ipynb",
                            "#@LISTEN=queue/BAD\n\ndef on_bad():\n    return missing_name +\n")

        found = self.loaded()["on_bad"]

        self.assertIsInstance(found["code"], str, "the code is kept as text, not as a callable")
        self.assertIn("tb", found)

    def test_a_cell_without_a_function_is_ignored(self):
        self.write_notebook("nofunc.ipynb", "#@LISTEN=queue/NONE\nX = 1\n")
        self.assertEqual(self.loaded(), {})

    def test_the_definition_is_published_in_the_lambda_index(self):
        self.write_notebook("orders.ipynb",
                            "#@LISTEN=queue/ORDERS\n\ndef on_orders():\n    return 'ok'\n")

        found = self.loaded()["on_orders"]

        index, identifier, body = self.es.writes[0]
        self.assertEqual(index, "nyx_lambda")
        self.assertEqual(identifier, ("r" + RUNNER + "_" + found["function"]).lower())
        published = json.loads(body)
        self.assertNotIn("code", published, "the code is not published, only its definition")
        self.assertEqual(published["type"], "message")

    def test_a_cron_lambda_is_published_as_a_clock(self):
        self.write_notebook("timers.ipynb", "#@INTERVAL=5m\n\ndef every_five():\n    return 1\n")
        self.loaded()
        published = json.loads(self.es.writes[0][2])
        self.assertEqual(published["type"], "cron")
        self.assertEqual(published["type_icon"], "regular/clock")

    def test_the_next_run_is_restored_from_redis_but_never_in_the_past(self):
        self.write_notebook("timers.ipynb", "#@INTERVAL=5m\n\ndef every_five():\n    return 1\n")
        lambdas.loadConfig()
        function = lambdas.lambdas[0]["function"]
        self.redis.store["lambda_" + RUNNER + "_" + function + "_nextrun"] = \
            b"2020-01-01T00:00:00+00:00"

        lambdas.loadConfig()

        next_run = lambdas.lambdas[0]["nextrun"]
        self.assertGreater(next_run, datetime.datetime.now(datetime.timezone.utc)
                           - datetime.timedelta(seconds=5))


class MessageReceivedTests(LambdaTestCase):
    """A message lands on a topic: every lambda subscribed to it runs, and each run is logged."""

    def subscribe(self, code, **overrides):
        lamb = {"function": "notebooksordersipynb_on_orders", "orgfunction": "on_orders",
                "code": code, "newcode": "def x():\n    pass", "runs": 0, "errors": 0,
                "return": "", "crashed": 0, "topics": ["queue/ORDERS"]}
        lamb.update(overrides)
        lambdas.lambdas = [lamb]
        lambdas.lambdasht = {"queue/ORDERS": [lamb]}
        return lamb

    def test_the_lambda_runs_and_its_result_comes_back(self):
        lamb = self.subscribe(lambda: "done")

        result = lambdas.messageReceived("queue/ORDERS", "payload", {"header": "1"})

        self.assertEqual(result, "done")
        self.assertEqual(lamb["return"], "done")
        self.assertEqual(lamb["runs"], 1)
        self.assertEqual(lamb["crashed"], 0)
        self.assertEqual(lamb["result_icon"], "check>green")
        self.assertEqual(lamb["type"], "message")
        self.assertNotIn("starttime", lamb, "the start time does not outlive the run")
        self.assertIn("log_uuid", lamb)

    def test_the_run_is_logged_with_the_message_and_its_headers(self):
        self.subscribe(lambda: "done")

        lambdas.messageReceived("queue/ORDERS", "payload", {"header": "1"})

        index, identifier, body = self.es.writes[0]
        self.assertEqual(index, "nyx_lambdalog")
        self.assertEqual(body["function"], "notebooksordersipynb_on_orders")
        self.assertEqual(body["message"], "payload")
        self.assertIn("header", body["headers"])
        self.assertEqual(body["runs"], 1)

    def test_a_crashing_lambda_is_recorded_as_a_failure(self):
        def boom():
            raise ValueError("boom")

        lamb = self.subscribe(boom)

        lambdas.messageReceived("queue/ORDERS", "payload", {})

        self.assertEqual(lamb["return"], "FAILURE")
        self.assertEqual(lamb["errors"], 1)
        self.assertEqual(lamb["crashed"], 1)
        self.assertEqual(lamb["result_icon"], "times>red")
        self.assertEqual(self.es.writes[0][2]["errors"], 1)

    def test_a_lambda_that_did_not_compile_is_reported_without_being_called(self):
        lamb = self.subscribe("def on_orders(:\n", tb="SyntaxError")

        lambdas.messageReceived("queue/ORDERS", "payload", {})

        self.assertEqual(lamb["return"], "COMPILE ERROR")
        self.assertEqual(lamb["crashed"], 1)
        self.assertEqual(lamb["result_icon"], "code>red")

    def test_a_restapi_destination_marks_the_run_as_a_call(self):
        lamb = self.subscribe(lambda: 1)
        lambdas.lambdasht = {"/restapi/calc": [lamb]}

        lambdas.messageReceived("/restapi/calc", "{}", {})

        self.assertEqual(lamb["type_icon"], "satellite-dish")

    def test_a_message_on_the_command_topic_is_a_command(self):
        with mock.patch.object(lambdas, "handleCommand") as command:
            command.return_value = "handled"
            result = lambdas.messageReceived("/topic/NYX_LAMBDA_COMMAND", "{}", {})

        self.assertEqual(result, "handled")
        command.assert_called_once()


class HandleCommandTests(LambdaTestCase):

    def command(self, **fields):
        payload = {"runner": RUNNER, "action": "add", "notebook": "orders.ipynb",
                   "function": "on_orders", "type": "message",
                   "parameters": ["queue/ORDERS"]}
        payload.update(fields)
        lambdas.handleCommand(json.dumps(payload), {})

    def test_a_command_for_another_runner_is_ignored(self):
        with mock.patch.object(lambdas, "createNotebook") as create:
            self.command(runner="someone-else")

        create.assert_not_called()
        self.assertEqual(self.broker.messages, [])

    def test_an_add_command_writes_the_notebook(self):
        with mock.patch.object(lambdas, "createNotebook") as create:
            self.command()

        create.assert_called_once_with("orders.ipynb", "on_orders", "message", "queue/ORDERS")

    def test_an_execute_command_calls_the_lambda_and_publishes_the_answer(self):
        lamb = {"function": "f", "orgfunction": "on_api", "code": lambda: "answer",
                "newcode": "", "runs": 0, "errors": 0, "return": "", "crashed": 0,
                "topics": [], "restapi": "calc", "interval": 0, "crontab": None}
        lambdas.lambdas = [lamb]
        lambdas.lambdasht = {"/restapi/calc": [lamb]}

        self.command(action="execute", restapi="calc", body={"site": "Namur"})

        published = json.loads(self.broker.sent_to("/topic/NYX_LAMBDA_RESTAPI")[0])
        self.assertEqual(published["return"], json.dumps("answer"))


class SaveLogTests(LambdaTestCase):

    def lamb(self, **overrides):
        record = {"orgfunction": "on_orders", "function": "f", "errors": 0, "return": "ok",
                  "runs": 3, "duration": 12.5, "crashed": 0}
        record.update(overrides)
        return record

    def test_the_log_lands_on_the_lambda_log_index(self):
        lambdas.save_log(["a line"], "uuid-1", "f", RUNNER, self.lamb())

        index, identifier, body = self.es.writes[0]
        self.assertEqual((index, identifier), ("nyx_lambdalog", "uuid-1"))
        self.assertEqual(body["logs"], ["a line"])
        self.assertEqual(body["runs"], 3)
        self.assertNotIn("message", body)

    def test_the_message_is_kept_truncated_with_its_headers(self):
        lambdas.save_log([], "uuid-2", "f", RUNNER, self.lamb(), message="x" * 600,
                         headers={"h": "1"})

        body = self.es.writes[0][2]
        self.assertEqual(len(body["message"]), 512)
        self.assertIn("h", body["headers"])

    def test_a_lambda_that_keeps_its_input_writes_it_on_disk(self):
        lambdas.save_log([], "uuid-3", "f", RUNNER, self.lamb(saveinput=True),
                         message="payload", headers={})

        self.assertTrue(os.path.exists(os.path.join(self.directory, "inputs", "on_orders",
                                                    "uuid-3.txt")))

    def test_an_unreachable_cluster_does_not_lose_the_run(self):
        self.es.index = mock.Mock(side_effect=ConnectionError("cluster down"))
        lambdas.save_log([], "uuid-4", "f", RUNNER, self.lamb())     # must not raise


class UpdateStatsTests(LambdaTestCase):

    def test_a_lambda_that_ran_is_pushed_and_reset(self):
        lamb = {"function": "f", "orgfunction": "on_orders", "code": lambda: 1, "runs": 2,
                "errors": 1, "return": "ok", "crashed": 0, "log_uuid": "u1"}
        lambdas.lambdas = [lamb]

        lambdas.updateStats()

        self.assertEqual(len(self.es.bulk_calls), 1)
        body = self.es.bulk_calls[0][0]
        self.assertIn('"update"', body)
        self.assertIn("ctx._source.runs += params.runs", body)
        self.assertNotIn('"code"', body, "the code is not pushed back to the cluster")
        self.assertEqual((lamb["runs"], lamb["errors"]), (0, 0), "the counters restart")

    def test_a_lambda_that_did_not_run_is_not_pushed(self):
        lambdas.lambdas = [{"function": "f", "orgfunction": "x", "code": lambda: 1, "runs": 0,
                            "errors": 0, "return": "", "crashed": 0}]

        lambdas.updateStats()

        self.assertEqual(self.es.bulk_calls, [])

    def test_the_next_run_is_pushed_when_one_is_known(self):
        lamb = {"function": "f", "orgfunction": "x", "code": lambda: 1, "runs": 1, "errors": 0,
                "return": "", "crashed": 0, "log_uuid": "u",
                "nextrun": datetime.datetime(2026, 11, 10, tzinfo=datetime.timezone.utc)}
        lambdas.lambdas = [lamb]

        lambdas.updateStats()

        body = self.es.bulk_calls[0][0]
        self.assertIn("ctx._source.nextrun", body)
        self.assertIn("2026-11-10", body)


class CheckIntervalsTests(LambdaTestCase):

    def test_one_tick_runs_the_lambda_that_is_due_and_reschedules_it(self):
        calls = []

        def due():
            calls.append("ran")
            return "ok"

        lamb = {"function": "every_five", "orgfunction": "every_five", "code": due,
                "newcode": "", "runs": 0, "errors": 0, "return": "", "crashed": 0,
                "interval": 60, "crontab": None, "topics": []}
        lambdas.lambdas = [lamb]
        lambdas.thread_check_intervals = _Once()

        lambdas.check_intervals_and_cron()

        self.assertEqual(calls, ["ran"])
        self.assertEqual(lamb["runs"], 1)
        self.assertEqual(lamb["return"], "ok")
        self.assertEqual(lamb["type"], "cron")
        self.assertTrue(any(key.startswith("lambda_" + RUNNER + "_every_five_nextrun")
                            for key, _ in self.redis.sets))
        self.assertEqual(len(self.es.writes), 1, "the run is logged")

    def test_a_lambda_without_a_trigger_is_left_alone(self):
        lamb = {"function": "manual", "orgfunction": "manual", "code": lambda: 1, "newcode": "",
                "runs": 0, "errors": 0, "return": "", "crashed": 0, "interval": 0,
                "crontab": None, "topics": []}
        lambdas.lambdas = [lamb]
        lambdas.thread_check_intervals = _Once()

        lambdas.check_intervals_and_cron()

        self.assertEqual(lamb["runs"], 0)
        self.assertEqual(self.es.writes, [])


class _Once:
    """A truthy flag that lets the `while` loop run exactly once."""

    def __init__(self):
        self.reads = 0

    def __bool__(self):
        self.reads += 1
        return self.reads == 1


class CheckRequirementsTests(LambdaTestCase):

    def setUp(self):
        super().setUp()
        lambdas.requirementstime = 0
        self.saved_requirements = lambdas.requirementstime
        self.saved_subprocess = lambdas.subprocess
        self.popens = []
        lambdas.subprocess = _FakeSubprocess(self.popens)

    def tearDown(self):
        lambdas.subprocess = self.saved_subprocess
        lambdas.requirementstime = self.saved_requirements

    def write_requirements(self, content):
        with open(os.path.join(self.directory, "requirements.txt"), "w") as handle:
            handle.write(content)

    def test_the_first_run_installs_and_marks_the_notebook_directory(self):
        self.write_requirements("pandas==2.2.3\n")

        changed = lambdas.checkIfRequirementsChanged()

        self.assertTrue(changed)
        self.assertEqual(len(self.popens), 1)
        self.assertTrue(os.path.exists(os.path.join(self.directory, "firstrun.txt")))

    def test_unchanged_requirements_do_not_reinstall(self):
        self.write_requirements("pandas==2.2.3\n")
        lambdas.checkIfRequirementsChanged()

        changed = lambdas.checkIfRequirementsChanged()

        self.assertFalse(changed)
        self.assertEqual(len(self.popens), 1, "pip ran once")

    def test_changed_requirements_are_installed_again(self):
        self.write_requirements("pandas==2.2.3\n")
        lambdas.checkIfRequirementsChanged()
        with open(os.path.join(self.directory, "requirements.txt"), "w") as handle:
            handle.write("pandas==2.2.4\n")
        lambdas.requirementstime = 0

        self.assertTrue(lambdas.checkIfRequirementsChanged())
        self.assertEqual(len(self.popens), 2)

    def test_a_fresh_volume_without_requirements_installs_nothing(self):
        # `firstrun.txt` is written, then the copy of requirements.txt used to raise: on a new
        # volume without that file, the lambda crashed at startup.
        self.assertFalse(lambdas.checkIfRequirementsChanged())

        self.assertEqual(self.popens, [])
        self.assertTrue(os.path.exists(os.path.join(self.directory, "firstrun.txt")))

    def test_requirements_that_disappear_are_not_reinstalled(self):
        # Once the first run has happened, a notebook directory without requirements.txt simply
        # has nothing to install.
        self.write_requirements("pandas==2.2.3\n")
        lambdas.checkIfRequirementsChanged()
        os.remove(os.path.join(self.directory, "requirements.txt"))

        self.assertFalse(lambdas.checkIfRequirementsChanged())
        self.assertEqual(len(self.popens), 1)


class _FakeSubprocess:
    """Just enough of `subprocess` for the pip install."""

    PIPE = -1
    STDOUT = -2

    def __init__(self, recorded):
        self.recorded = recorded

    def Popen(self, command, cwd=None, **kwargs):
        self.recorded.append(command)

        class Process:
            returncode = 0

            def communicate(inner):
                return b"installed\n", None

        return Process()


if __name__ == "__main__":
    unittest.main()
