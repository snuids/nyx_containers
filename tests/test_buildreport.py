"""The python/xlsx report builder: the notebook cells it runs and the log it keeps.

`nyx_buildreport.py` used to run its whole generation at import: importing it connected to
`elasticsearch:9200`, read a notebook and executed it. It is now a `main()` the runner calls and a
test can call with a cluster double, so what it does with a job — the parameters it converts, the
cells it executes, the error it reports — can be asserted.
"""

import datetime
import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd
from docx import Document

sys.path.insert(0, str(Path(__file__).resolve().parent))

from harness import FakeES, Harness                                       # noqa: E402

SCENARIO = Harness()
builder = SCENARIO.load("nyx_buildreport").module


def utc(written):
    """A date the builder wrote as text, read back in UTC (it writes in the container's zone)."""
    return datetime.datetime.fromisoformat(written).astimezone(datetime.timezone.utc)


def notebook(path, *cells):
    """Writes a minimal .ipynb whose code cells are the strings given."""
    document = {"metadata": {}, "nbformat": 4, "nbformat_minor": 2,
                "cells": [{"cell_type": "code", "metadata": {}, "outputs": [],
                           "execution_count": None,
                           "source": cell.splitlines(keepends=True)} for cell in cells]}
    with open(path, "w") as handle:
        json.dump(document, handle)


class ReplaceTextTests(unittest.TestCase):

    def test_a_placeholder_is_replaced_and_an_unknown_one_is_left_alone(self):
        text = builder.replaceText({"site": "Namur", "count": 3}, "Site ${site}, ${count} pts, ${x}")
        self.assertEqual(text, "Site Namur, 3 pts, ${x}")

    def test_a_whole_percentage_loses_its_trailing_zero(self):
        # A cell prints "12.0 %": the trailing zero is dropped, but only along a real replacement.
        self.assertEqual(builder.replaceText({"p": "12.0 %"}, "${p} de mieux"), "12 % de mieux")

    def test_a_key_that_cannot_be_used_in_a_placeholder_is_skipped(self):
        # The replacement is built as "${" + key + "}": a key that is not a string raises, and the
        # text has to come back untouched rather than losing the rest of the substitutions.
        self.assertEqual(builder.replaceText({1: "x", "ok": "yes"}, "${1} ${ok}"), "${1} yes")

    def test_the_logger_calls_of_a_cell_are_rewritten_to_the_builder_helpers(self):
        self.assertEqual(builder.computeNewCode("logger.info('x')"), "logger_info('x')")


class CreateTableTests(unittest.TestCase):
    """The table helper the notebook cells call: header from the columns, one row per record."""

    def setUp(self):
        builder.template = Document()
        self.addCleanup(setattr, builder, "template", None)

    def test_the_header_and_the_rows_come_from_the_frame(self):
        frame = pd.DataFrame([{"site": "Namur", "count": 12}, {"site": "Liège", "count": 7}])
        paragraph = builder.template.add_paragraph()

        table = builder.create_table(paragraph, frame, title="Sites")

        cells = [[cell.text for cell in row.cells] for row in table.rows]
        self.assertEqual(cells, [["site", "count"], ["Namur", "12"], ["Liège", "7"]])
        self.assertEqual(paragraph.text, "Sites")

    def test_the_table_lands_after_its_paragraph(self):
        paragraph = builder.template.add_paragraph("before")
        builder.create_table(paragraph, pd.DataFrame([{"a": 1}]))

        body = builder.template.element.body
        children = list(body)
        position = next(index for index, child in enumerate(children)
                        if child is paragraph._p)
        self.assertEqual(children[position + 1].tag.split("}")[-1], "tbl")


class MainTests(unittest.TestCase):
    """`main()`: the parameters it converts, the cells it runs, and what it logs."""

    def setUp(self):
        self.directory = tempfile.mkdtemp()
        self.saved_cwd = os.getcwd()
        os.chdir(self.directory)
        os.makedirs("reports/notebooks")
        os.makedirs("generated")
        self.es = FakeES()
        self.addCleanup(self.restore)

    def restore(self):
        os.chdir(self.saved_cwd)
        shutil.rmtree(self.directory)

    def job(self, notebook_name="report1", parameters=None):
        return json.dumps({
            "id": "id_42",
            "creds": {"token": "token-42",
                      "user": {"id": "alice@equans.com", "login": "alice", "language": "en"}},
            "report": {"title": "Monthly", "notebook": notebook_name, "reportType": "notebook",
                       "output": ["xlsx"], "generatePDF": False,
                       "parameters": parameters if parameters is not None else [
                           {"name": "period", "type": "interval",
                            "value": ["2026-11-01T00:00:00Z", "2026-11-08T00:00:00Z"]},
                           {"name": "day", "type": "date", "value": "2026-11-09T00:00:00Z"},
                           {"name": "site", "type": "text", "value": "Namur"}]},
            "treatment": {}, "output": os.path.join(self.directory, "generated", "id_42"),
        })

    def seen(self):
        with open("seen_params.json") as handle:
            return json.load(handle)

    def test_the_parameters_are_converted_into_dates_and_values(self):
        notebook("reports/notebooks/report1.ipynb",
                 "#@ONLOAD\n"
                 "import json\n"
                 "with open('seen_params.json', 'w') as handle:\n"
                 "    handle.write(json.dumps({key: str(value) for key, value in params.items()}))\n")

        code = builder.main(["nyx_buildreport.py", self.job()], client=self.es)

        self.assertEqual(code, None, "the script returns nothing when it succeeded")
        seen = self.seen()
        self.assertNotIn("period", seen)
        # Compared in UTC: the builder converts to the container's zone, which is the point.
        self.assertEqual(utc(seen["period_start"]), datetime.datetime(2026, 11, 1,
                                                                    tzinfo=datetime.timezone.utc))
        self.assertEqual(utc(seen["period_end"]), datetime.datetime(2026, 11, 8,
                                                                  tzinfo=datetime.timezone.utc))
        self.assertEqual(utc(seen["day"]), datetime.datetime(2026, 11, 9,
                                                           tzinfo=datetime.timezone.utc))
        self.assertEqual(seen["site"], "Namur")

    def test_a_parameter_block_the_report_does_not_know_is_left_as_it_is(self):
        notebook("reports/notebooks/report1.ipynb",
                 "#@ONLOAD\n"
                 "import json\n"
                 "with open('seen_params.json', 'w') as handle:\n"
                 "    handle.write(json.dumps({key: str(value) for key, value in params.items()}))\n")

        builder.main(["nyx_buildreport.py", self.job(
            parameters=[{"name": "n", "type": "number", "value": 5}])], client=self.es)

        self.assertEqual(self.seen(), {"n": "5"})

    def test_the_onload_cell_runs_and_the_log_is_saved_on_the_report(self):
        notebook("reports/notebooks/report1.ipynb",
                 "#@ONLOAD\nopen('onload.txt', 'w').write('ran')\n")

        builder.main(["nyx_buildreport.py", self.job()], client=self.es)

        self.assertTrue(os.path.exists("onload.txt"), "the #@ONLOAD cell must be executed")
        index, identifier, body = self.es.writes[0]
        self.assertEqual((index, identifier), ("nyx_reportlog", "id_42"))
        self.assertIn("@timestamp", body)
        self.assertEqual(body["logs"], [], "a run without logger_* output keeps an empty log")

    def test_a_paragraph_cell_is_collected_for_the_document(self):
        # The python builder collects the `${name}` blocks; they are unused here but the cells
        # must still be read without crashing the run.
        notebook("reports/notebooks/report1.ipynb",
                 "#@PARAGRAPH=summary\nlogger.info('built')\n")

        builder.main(["nyx_buildreport.py", self.job()], client=self.es)

        self.assertEqual(self.es.writes[0][1], "id_42")

    def test_a_missing_notebook_stops_the_run(self):
        with self.assertRaises(SystemExit) as stopped:
            builder.main(["nyx_buildreport.py", self.job()], client=self.es)

        self.assertEqual(stopped.exception.code, 1)
        self.assertEqual(self.es.writes, [])

    def test_a_crashing_cell_is_reported_and_the_log_saved_before_the_script_gives_up(self):
        notebook("reports/notebooks/report1.ipynb",
                 "#@ONLOAD\nlogger.info('about to fail')\nraise ValueError('boom')\n")

        with self.assertRaises(Exception) as failure:
            builder.main(["nyx_buildreport.py", self.job()], client=self.es)

        self.assertIn("ERROR COMMON", str(failure.exception))
        index, identifier, body = self.es.writes[0]
        self.assertEqual(identifier, "id_42")
        self.assertTrue(any("about to fail" in line for line in body["logs"]),
                        "what the cell logged before crashing is kept")
        self.assertTrue(any("ERROR" in line for line in body["logs"]))

    def test_the_job_message_is_not_logged_with_its_token(self):
        notebook("reports/notebooks/report1.ipynb", "#@ONLOAD\npass\n")

        with self.assertLogs(level="INFO") as captured:
            builder.main(["nyx_buildreport.py", self.job()], client=self.es)

        self.assertNotIn("token-42", "\n".join(captured.output))


if __name__ == "__main__":
    unittest.main()
