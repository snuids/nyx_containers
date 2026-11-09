"""The lambda runner: the notebook writer, the code rewriter, and the change detector.

A lambda is edited in the interface, which writes a notebook; the runner turns that notebook into
a callable and reloads it when the file changes. `computeNewCode` is what renames the function so
two notebooks cannot collide, and `checkIfChanged` is what decides a reload — if it stopped
detecting a change, a lambda would keep running yesterday's code without a word.
"""

import json
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from harness import FakeES, FakeRedis, Harness                           # noqa: E402

SCENARIO = Harness()
lambdas = SCENARIO.load("nyx_lambda").module


class GetELKVersionTests(unittest.TestCase):

    def test_the_major_version_is_read_from_the_cluster(self):
        self.assertEqual(lambdas.getELKVersion(FakeES(version="6.8.23")), 6)
        self.assertEqual(lambdas.getELKVersion(FakeES(version="8.19.23")), 8)


class ComputeNewCodeTests(unittest.TestCase):
    """The function of a notebook is prefixed with the notebook name: one namespace per lambda."""

    def test_the_function_is_prefixed_with_the_sanitised_notebook_name(self):
        newcode = "\ndef hello():\n    logger.info('hi')\n    return 1\n"

        code, function = lambdas.computeNewCode("My Notebook!", newcode, "")

        self.assertEqual(function, "mynotebook_hello")
        self.assertIn("def mynotebook_hello():", code)
        self.assertIn("    return 1", code)

    def test_logger_calls_are_rewritten_to_the_runner_helpers(self):
        code, _ = lambdas.computeNewCode("nb", "\ndef f():\n    logger.warning('x')\n", "")

        # `logger.warning` would be a NameError in the exec'd namespace: the runner exposes
        # logger_info/logger_warning instead.
        self.assertIn("logger_warning", code)
        self.assertNotIn("logger.", code)

    def test_the_common_block_is_indented_inside_the_function(self):
        code, _ = lambdas.computeNewCode("nb", "\ndef f():\n    return 1\n", "import math\nX = 1")

        self.assertIn("    import math\n    X = 1\n", code)
        # the body still follows the injected block
        self.assertIn("    return 1", code)

    def test_code_without_a_function_is_refused(self):
        code, function = lambdas.computeNewCode("nb", "X = 1\nY = 2\n", "")

        self.assertEqual(function, "UNABLETOFINDFUNC")
        self.assertEqual(code, "NOFUNC")


class CreateNotebookTests(unittest.TestCase):
    """The interface's "new lambda" button: the template that lands on disk."""

    def setUp(self):
        self.directory = tempfile.mkdtemp()
        self.saved_path = lambdas.path
        lambdas.path = self.directory
        self.addCleanup(self.restore)

    def restore(self):
        lambdas.path = self.saved_path
        for name in os.listdir(self.directory):
            os.remove(os.path.join(self.directory, name))
        os.rmdir(self.directory)

    def stored(self, name):
        with open(os.path.join(self.directory, name)) as handle:
            return json.load(handle)

    def test_a_message_lambda_gets_a_listen_cell_and_a_stub(self):
        lambdas.createNotebook("orders.ipynb", "process", "message", "queue/ORDERS")

        notebook = self.stored("orders.ipynb")
        sources = ["".join(cell["source"]) for cell in notebook["cells"]]
        self.assertTrue(any("#@LISTEN=queue/ORDERS" in source for source in sources))
        self.assertTrue(any("def process():" in source for source in sources))
        self.assertEqual(notebook["nbformat"], 4)

    def test_a_cron_lambda_gets_an_interval_cell(self):
        lambdas.createNotebook("nightly.ipynb", "purge", "cron", "1h")

        sources = ["".join(cell["source"]) for cell in self.stored("nightly.ipynb")["cells"]]
        self.assertTrue(any("#@INTERVAL=1h" in source for source in sources))

    def test_an_existing_notebook_is_updated_rather_than_replaced(self):
        first = os.path.join(self.directory, "orders.ipynb")
        lambdas.createNotebook("orders.ipynb", "process", "message", "queue/ORDERS")
        with open(first) as handle:
            original = json.load(handle)
        original["cells"].append({"cell_type": "markdown", "metadata": {}, "source": ["kept"]})
        with open(first, "w") as handle:
            json.dump(original, handle)

        lambdas.createNotebook("orders.ipynb", "process", "message", "queue/ORDERS")

        sources = ["".join(cell["source"]) for cell in self.stored("orders.ipynb")["cells"]]
        self.assertIn("kept", sources, "the hand-written cell must survive an interface edit")


class CheckIfChangedTests(unittest.TestCase):
    """The reload decision: a notebook's mtime, remembered in Redis for thirty days."""

    def setUp(self):
        self.directory = tempfile.mkdtemp()
        self.notebook = os.path.join(self.directory, "orders.ipynb")
        with open(self.notebook, "w") as handle:
            handle.write("{}")
        self.saved = (lambdas.path, lambdas.curconfig)
        lambdas.path = self.directory
        lambdas.curconfig = ""
        lambdas.lambdaschanged = {}
        self.redis = FakeRedis()
        lambdas.redisserver = self.redis
        self.addCleanup(self.restore)

    def restore(self):
        lambdas.path, lambdas.curconfig = self.saved
        os.remove(self.notebook)
        os.rmdir(self.directory)

    def test_the_first_pass_reports_a_change(self):
        self.assertTrue(lambdas.checkIfChanged({}))

    def test_a_second_pass_with_nothing_touched_reports_none(self):
        lambdas.checkIfChanged({})
        self.assertFalse(lambdas.checkIfChanged({}))

    def test_a_touched_notebook_is_reported_and_remembered(self):
        lambdas.checkIfChanged({})
        os.utime(self.notebook, (time.time() + 10, time.time() + 10))

        self.assertTrue(lambdas.checkIfChanged({}))
        self.assertIn("orders.ipynb", lambdas.lambdaschanged)

    def test_the_mtime_is_kept_in_redis_for_a_month(self):
        lambdas.checkIfChanged({})
        keys = [key for key, _ in self.redis.sets if key.startswith("file_")]
        self.assertEqual(len(keys), 1)
        self.assertEqual(self.redis.sets[0][1], 86400 * 30)


if __name__ == "__main__":
    unittest.main()
