"""The docx report builder: the template it fills and the document it writes.

Same shape as the python builder — a `main()` the runner calls, a `client` a test can replace —
plus the document helpers that the notebook cells call from inside their cells (`create_table`,
`insert_paragraph_after`, `resize_table`). The template placeholders are the contract with the
report author: `${name}` for a parameter, `${block}` for a cell that writes its own paragraph.
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
from docx.shared import Inches

sys.path.insert(0, str(Path(__file__).resolve().parent))

from harness import FakeES, Harness                                       # noqa: E402

SCENARIO = Harness()
builder = SCENARIO.load("nyx_buildreport_doc").module


def notebook(path, *cells):
    document = {"metadata": {}, "nbformat": 4, "nbformat_minor": 2,
                "cells": [{"cell_type": "code", "metadata": {}, "outputs": [],
                           "execution_count": None,
                           "source": cell.splitlines(keepends=True)} for cell in cells]}
    with open(path, "w") as handle:
        json.dump(document, handle)


class ComputestyleTests(unittest.TestCase):

    def test_the_level_becomes_a_word_style(self):
        self.assertEqual(builder.computestyle(0), "Title")
        self.assertEqual(builder.computestyle(1), "Heading 1")
        self.assertEqual(builder.computestyle(3), "Heading 3")
        self.assertEqual(builder.computestyle(), "Heading 1")


class ReplaceTextTests(unittest.TestCase):

    def test_a_parameter_placeholder_is_replaced(self):
        self.assertEqual(builder.replaceText({"site": "Namur"}, "Site : ${site}"), "Site : Namur")

    def test_a_percentage_loses_its_trailing_zero(self):
        self.assertEqual(builder.replaceText({"p": "12.0 %"}, "${p}"), "12 %")

    def test_a_key_that_is_not_a_string_is_skipped(self):
        self.assertEqual(builder.replaceText({1: "x"}, "${1}"), "${1}")


class DocumentHelpersTests(unittest.TestCase):
    """The helpers a notebook cell calls with `paragraph` and `template`."""

    def setUp(self):
        builder.template = Document()
        self.addCleanup(setattr, builder, "template", None)

    def test_a_paragraph_is_inserted_after_the_one_given(self):
        first = builder.template.add_paragraph("first")
        second = builder.template.add_paragraph("second")

        inserted = builder.insert_paragraph_after(first, text="inserted")

        body = [child for child in builder.template.element.body]
        self.assertEqual(body.index(inserted._p), body.index(first._p) + 1)
        self.assertEqual(body.index(second._p), body.index(inserted._p) + 1)
        self.assertEqual(inserted.text, "inserted")

    def test_a_table_is_filled_and_placed_after_its_paragraph(self):
        paragraph = builder.template.add_paragraph("intro")
        frame = pd.DataFrame([{"site": "Namur", "count": 12}, {"site": "Liège", "count": 7}])

        new_paragraph, table = builder.create_table(paragraph, frame, col_width=[2, 1])

        cells = [[cell.text for cell in row.cells] for row in table.rows]
        self.assertEqual(cells, [["site", "count"], ["Namur", "12"], ["Liège", "7"]])
        self.assertEqual(table.autofit, False)
        self.assertEqual(table.columns[0].cells[0].width, Inches(2))
        self.assertEqual(table.columns[1].cells[0].width, Inches(1))
        body = [child for child in builder.template.element.body]
        self.assertEqual(body[body.index(paragraph._p) + 1].tag.split("}")[-1], "p")
        self.assertEqual(body[body.index(paragraph._p) + 2].tag.split("}")[-1], "tbl")

    def test_a_table_without_widths_autofits(self):
        paragraph = builder.template.add_paragraph()
        _, table = builder.create_table(paragraph, pd.DataFrame([{"a": 1}]))
        self.assertEqual(table.autofit, True)

    def test_the_columns_of_an_existing_table_are_resized(self):
        table = builder.template.add_table(2, 2)

        builder.resize_table(table, [1.5, 2.5])

        self.assertEqual(table.autofit, False)
        self.assertEqual(table.columns[0].cells[0].width, Inches(1.5))
        self.assertEqual(table.columns[1].cells[1].width, Inches(2.5))


class MainTests(unittest.TestCase):

    def setUp(self):
        self.directory = os.path.realpath(tempfile.mkdtemp())
        self.saved_cwd = os.getcwd()
        os.chdir(self.directory)
        os.makedirs("reports/notebooks")
        os.makedirs("generated")
        self.es = FakeES()
        self.addCleanup(self.restore)

    def restore(self):
        os.chdir(self.saved_cwd)
        shutil.rmtree(self.directory)
        builder.template = None

    def template_document(self, name="monthly"):
        document = Document()
        document.add_paragraph("Rapport ${title} — ${missing}")
        table = document.add_table(rows=1, cols=1)
        table.cell(0, 0).paragraphs[0].text = "${tbl}"
        document.save("reports/notebooks/" + name + ".docx")

    def job(self, notebook_name="monthly"):
        return json.dumps({
            "id": "id_42",
            "creds": {"token": "token-42", "user": {"id": "a@b", "login": "a", "language": "fr"}},
            "report": {"title": "Mensuel", "notebook": notebook_name, "reportType": "notebook_doc",
                       "output": ["docx"], "generatePDF": False,
                       "parameters": [
                           {"name": "title", "type": "text", "value": "Mensuel"},
                           {"name": "site", "type": "text", "value": "Namur"}]},
            "treatment": {}, "output": os.path.join(self.directory, "generated", "id_42"),
        })

    def run_builder(self, argv=None):
        return builder.main(argv or ["nyx_buildreport_doc.py", self.job()], client=self.es)

    def saved(self):
        path = os.path.join(self.directory, "generated", "id_42.docx")
        self.assertTrue(os.path.exists(path), "the document must be written where the job says")
        return Document(path)

    def test_a_placeholder_is_replaced_by_its_parameter(self):
        self.template_document()
        notebook("reports/notebooks/monthly.ipynb", "#@ONLOAD\npass\n")

        self.run_builder()

        texts = [paragraph.text for paragraph in self.saved().paragraphs]
        self.assertIn("Rapport Mensuel — ${missing}", texts,
                      "an unknown placeholder is left for the author to see")

    def test_a_paragraph_cell_writes_its_own_text(self):
        self.template_document()
        notebook("reports/notebooks/monthly.ipynb",
                 "#@ONLOAD\npass\n",
                 "#@PARAGRAPH=tbl\n"
                 "paragraph.text = 'Cellule remplie : ' + str(params['site'])\n")

        self.run_builder()

        table = self.saved().tables[0]
        self.assertEqual(table.cell(0, 0).text, "Cellule remplie : Namur")

    def test_a_table_cell_without_a_cell_function_gets_the_parameters(self):
        document = Document()
        table = document.add_table(rows=1, cols=1)
        table.cell(0, 0).paragraphs[0].text = "Site : ${site}"
        document.save("reports/notebooks/monthly.docx")
        notebook("reports/notebooks/monthly.ipynb", "#@ONLOAD\npass\n")

        self.run_builder()

        self.assertEqual(self.saved().tables[0].cell(0, 0).text, "Site : Namur")

    def test_the_onload_cell_runs_and_the_log_lands_on_the_report(self):
        self.template_document()
        notebook("reports/notebooks/monthly.ipynb",
                 "#@ONLOAD\nlogger.info('cell executed')\nopen('onload.txt', 'w').write('ran')\n")

        self.run_builder()

        self.assertTrue(os.path.exists("onload.txt"))
        index, identifier, body = self.es.writes[0]
        self.assertEqual((index, identifier), ("nyx_reportlog", "id_42"))
        self.assertTrue(any("cell executed" in line for line in body["logs"]))

    def test_a_missing_document_stops_the_run(self):
        notebook("reports/notebooks/monthly.ipynb", "#@ONLOAD\npass\n")

        with self.assertRaises(SystemExit) as stopped:
            self.run_builder()

        self.assertEqual(stopped.exception.code, 1)
        self.assertEqual(self.es.writes, [])

    def test_a_missing_notebook_stops_the_run(self):
        self.template_document()

        with self.assertRaises(SystemExit) as stopped:
            self.run_builder()

        self.assertEqual(stopped.exception.code, 1)

    def test_a_crashing_cell_is_reported_and_the_log_saved(self):
        self.template_document()
        notebook("reports/notebooks/monthly.ipynb",
                 "#@ONLOAD\nlogger.info('before')\nraise ValueError('boom')\n")

        with self.assertRaises(Exception) as failure:
            self.run_builder()

        self.assertIn("ERROR COMMON", str(failure.exception))
        body = self.es.writes[0][2]
        self.assertTrue(any("before" in line for line in body["logs"]))
        self.assertTrue(any("ERROR" in line for line in body["logs"]))

    def test_the_document_is_left_alone_when_a_paragraph_cell_crashes(self):
        self.template_document()
        notebook("reports/notebooks/monthly.ipynb",
                 "#@ONLOAD\npass\n",
                 "#@PARAGRAPH=tbl\nraise ValueError('boom')\n")

        with self.assertRaises(Exception):
            self.run_builder()

        self.assertFalse(os.path.exists(os.path.join(self.directory, "generated", "id_42.docx")),
                         "a crashed report must not leave a half-filled document behind")


if __name__ == "__main__":
    unittest.main()
