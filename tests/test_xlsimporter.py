"""The XLS importer: a spreadsheet on the broker becomes an index and its documents.

It is the only path that turns a customer file into an index, and it decides the mapping from the
column types. A wrong type is not a failed import: the index is created, the documents go in, and
the chart silently stops returning the column.
"""

import base64
import datetime
import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

import warnings

import openpyxl

sys.path.insert(0, str(Path(__file__).resolve().parent))

from harness import FakeBroker, FakeES, Harness                           # noqa: E402

SCENARIO = Harness()
importer = SCENARIO.load("nyx_xlsimporter").module

# The importer reads the dtypes by position, which pandas 2.2 deprecates: the warning is noise
# in the test output and does not change what is sent.
# No `warnings.filterwarnings` here: the module used to read the dtypes by position, which pandas
# 2.2 deprecates and pandas 3 removes. The test below asserts the warning is gone for good.


def workbook(rows, sheet="Sheet1"):
    document = openpyxl.Workbook()
    sheet = document.active
    for row in rows:
        sheet.append(list(row))
    sink = os.path.join(tempfile.mkdtemp(), "book.xlsx")
    document.save(sink)
    with open(sink, "rb") as handle:
        content = handle.read()
    os.remove(sink)
    os.rmdir(os.path.dirname(sink))
    return base64.b64encode(content).decode("ascii")


class ImportTests(unittest.TestCase):

    def setUp(self):
        self.directory = tempfile.mkdtemp()
        os.makedirs(os.path.join(self.directory, "tmp"))
        self.saved_cwd = os.getcwd()
        os.chdir(self.directory)

        self.es = FakeES()
        self.broker = FakeBroker()
        importer.es = self.es
        importer.conn = self.broker

        self.addCleanup(self.restore)
        self.saved_pte = importer.pte.pandas_to_elastic

    def restore(self):
        os.chdir(self.saved_cwd)
        shutil.rmtree(self.directory)
        importer.es = None
        importer.conn = None
        importer.pte.pandas_to_elastic = self.saved_pte

    def receive(self, content, filename="Sites 2026.xlsx"):
        importer.messageReceived("FILE_UPLOAD_XLS", content, {"file": filename})

    def test_the_index_is_named_after_the_file(self):
        self.receive(workbook([("name", "count"), ("Namur", 12)]))

        self.assertEqual(self.es.indices.created[0][0], "sites2026",
                         "spaces stripped, dots cut, lower case")

    def test_the_mapping_follows_the_column_types(self):
        self.receive(workbook([
            ("name", "count", "ratio", "day"),
            ("Namur", 12, 1.5, datetime.datetime(2026, 11, 9)),
        ]))

        properties = self.es.indices.created[0][1]["mappings"]["properties"]
        self.assertEqual(properties["name"], {"type": "keyword"})
        self.assertEqual(properties["count"], {"type": "long"})
        self.assertEqual(properties["ratio"], {"type": "float"})
        self.assertEqual(properties["day"], {"type": "date"})

    def test_the_identifier_columns_are_not_mapped(self):
        self.receive(workbook([("_id", "name"), ("abc", "Namur")]))
        properties = self.es.indices.created[0][1]["mappings"]["properties"]
        self.assertNotIn("_id", properties)

    def test_an_existing_index_is_not_recreated(self):
        self.es.indices.existing.add("sites2026")
        self.receive(workbook([("name",), ("Namur",)]))
        self.assertEqual(self.es.indices.created, [])

    def test_the_rows_are_bulk_indexed_under_the_file_index(self):
        self.receive(workbook([("name", "count"), ("Namur", 12), ("Liège", 7)]))

        actions = self.es.bulk_actions()
        self.assertEqual([action["index"]["_index"] for action in actions],
                         ["sites2026", "sites2026"])
        self.assertEqual([document["name"] for document in self.es.bulk_documents()],
                         ["Namur", "Liège"])

    def test_an_index_column_in_the_file_wins(self):
        self.receive(workbook([("_index", "name"), ("reads", "Namur")]))

        self.assertEqual(self.es.indices.created[0][0], "reads")
        self.assertEqual(self.es.bulk_actions()[0]["index"]["_index"], "reads")

    def test_reading_the_column_types_raises_no_deprecation_warning(self):
        # pandas 2.2 warns on `series[index]` for a positional read; pandas 3 removes it. The
        # importer reads the dtypes with `.iloc`, so the mapping inference stays quiet.
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            self.receive(workbook([("name", "count"), ("Namur", 12)]))

        future = [w for w in caught if issubclass(w.category, FutureWarning)]
        self.assertEqual([str(w.message) for w in future], [])

    def test_the_import_is_traced_on_the_log_queue(self):
        self.receive(workbook([("name",), ("Namur",)]))

        messages = [json.loads(body) for body in self.broker.sent_to("/queue/NYX_LOG")]
        self.assertEqual(messages[0]["message"], "Import of file [Sites 2026.xlsx] started.")
        self.assertTrue(any("Creating index [sites2026]." == message["message"]
                            for message in messages))
        self.assertIn("finished", messages[-1]["message"])
        self.assertEqual(messages[-1]["module"], "XLS_IMPORTER")


if __name__ == "__main__":
    unittest.main()
