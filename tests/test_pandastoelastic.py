"""`pandas_to_elastic`: the bulk body the XLS importer sends.

The importer is the only path that turns a spreadsheet into documents, and the index it writes is
the one the customer charts read. The function swallows its exceptions (`except:` logs and
returns), so a test on the return value would pass even when nothing is sent: these tests assert
on the bulk body the cluster would have received.
"""

import datetime
import sys
import unittest
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

from harness import FakeES                                                    # noqa: E402

import lib.pandastoelastic as pte                                             # noqa: E402


class PandasToElasticTests(unittest.TestCase):

    def setUp(self):
        self.es = FakeES()
        self.saved_bulk = pte.pd.DataFrame

    def store(self, frame):
        pte.pandas_to_elastic(self.es, frame)
        return self.es.bulk_documents(), self.es.bulk_actions()

    def test_each_row_becomes_an_action_and_a_document(self):
        frame = pd.DataFrame([
            {"_index": "sites", "_id": "1", "name": "Namur", "capacity": 12},
            {"_index": "sites", "_id": "2", "name": "Liège", "capacity": 7},
        ])

        documents, actions = self.store(frame)

        self.assertEqual(len(documents), 2)
        self.assertEqual(documents[0], {"name": "Namur", "capacity": 12})
        self.assertEqual(actions[0]["index"]["_index"], "sites")
        self.assertEqual(actions[0]["index"]["_id"], "1")
        self.assertEqual(actions[0]["index"]["_type"], "_doc")
        # `_index` and `_id` address the document, they are not fields of it.
        self.assertNotIn("_index", documents[0])
        self.assertNotIn("_id", documents[0])

    def test_a_row_without_an_id_is_indexed_without_one(self):
        _, actions = self.store(pd.DataFrame([{"_index": "sites", "name": "Namur"}]))
        self.assertNotIn("_id", actions[0]["index"])

    def test_an_epoch_column_becomes_the_document_timestamp(self):
        documents, _ = self.store(pd.DataFrame([{
            "_index": "reads", "_timestamp": 1762684800000, "value": 3,
        }]))
        self.assertEqual(documents[0]["@timestamp"], 1762684800000)
        self.assertNotIn("_timestamp", documents[0])

    def test_a_datetime_column_becomes_epoch_milliseconds(self):
        # A tz-aware value, because pandas and Python disagree on what a naive datetime with no
        # zone means (pandas reads it as UTC, `datetime.timestamp()` as the container's zone).
        moment = datetime.datetime(2026, 11, 9, 10, 30, 0, tzinfo=datetime.timezone.utc)
        documents, _ = self.store(pd.DataFrame([{
            "_index": "reads", "_timestamp": moment, "value": 3,
        }]))
        self.assertEqual(documents[0]["@timestamp"], int(pd.Timestamp(moment).timestamp() * 1000))
        self.assertNotIn("_timestamp", documents[0])

    def test_a_missing_value_is_left_out_rather_than_sent_as_nat(self):
        # A NaT in the body makes the whole bulk fail in the cluster: the row is dropped instead.
        frame = pd.DataFrame([
            {"_index": "reads", "value": 1, "comment": pd.NaT},
            {"_index": "reads", "value": 2, "comment": "kept"},
        ])

        documents, _ = self.store(frame)

        self.assertEqual(len(documents), 2)
        self.assertNotIn("comment", documents[0])
        self.assertEqual(documents[1]["comment"], "kept")

    def test_an_empty_frame_sends_nothing(self):
        documents, actions = self.store(pd.DataFrame())
        self.assertEqual(documents, [])
        self.assertEqual(actions, [])
        self.assertEqual(self.es.bulk_calls, [])

    def test_a_body_over_half_a_megabyte_is_flushed_mid_frame(self):
        padded = "x" * 600000
        frame = pd.DataFrame([
            {"_index": "reads", "value": 1, "comment": padded},
            {"_index": "reads", "value": 2, "comment": "small"},
        ])

        self.store(frame)

        self.assertEqual(len(self.es.bulk_calls), 2, "the first row fills the window on its own")
        self.assertEqual(self.es.bulk_calls[0][1].get("request_timeout"), 30)
        self.assertEqual(len(self.es.bulk_documents()), 2)

    def test_bulk_errors_are_collected_rather_than_raised(self):
        # The importer reports "Import finished" whatever happened: the errors have to be visible
        # in the bulk result, the log is the only place they surface.
        self.es.bulk_result = {"errors": True, "items": [
            {"index": {"_id": "1", "error": {"type": "mapper_parsing_exception"}}},
            {"index": {"_id": "2", "status": 201}},
        ]}

        with self.assertLogs(level="INFO") as captured:
            self.store(pd.DataFrame([{"_index": "reads", "value": 1}]))

        self.assertTrue(any("mapper_parsing_exception" in line for line in captured.output))

    def test_a_cluster_error_does_not_raise(self):
        class Unreachable(FakeES):
            def bulk(self, body=None, **kwargs):
                raise ConnectionError("cluster down")

        pte.pandas_to_elastic(Unreachable(), pd.DataFrame([{"_index": "reads", "value": 1}]))
        # nothing to assert beyond "it returned": the caller keeps going, by design


if __name__ == "__main__":
    unittest.main()
