"""`genericIntervalSearch`: the query a report builds, and the scroll that fills it.

The report builder reads its data through this function. Its body is assembled by hand, so the
one thing worth pinning down is what it sends: a `query_string`, an optional range on the time
field, the sort, and the `_id`/`_index` columns the template engine expects on every row.
"""

import datetime
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from harness import FakeES                                                    # noqa: E402

import lib.elastictopandas as etp                                             # noqa: E402


def page(hits, scroll_id="s1", total=0):
    return {"_scroll_id": scroll_id,
            "hits": {"total": total, "hits": hits}}


def hit(identifier, index="reads", **fields):
    document = {"_id": identifier, "_index": index}
    document.update(fields)
    return {"_id": identifier, "_index": index, "_source": document}


class GenericIntervalSearchTests(unittest.TestCase):

    def setUp(self):
        self.es = FakeES()

    def test_every_page_lands_in_the_frame_with_its_identifier(self):
        self.es.search_result = page([hit("1", value=10)], total=2)
        self.es.scroll_pages = [
            page([hit("2", value=20)], scroll_id="s2", total=2),
            page([], scroll_id="s3", total=2),
        ]

        frame = etp.genericIntervalSearch(self.es, "reads")

        self.assertEqual(len(frame), 2)
        self.assertEqual(list(frame["_id"]), ["1", "2"])
        self.assertEqual(list(frame["_index"]), ["reads", "reads"])
        self.assertEqual(len(self.es.scrolls), 2, "the scroll continues until a page comes back empty")

    def test_the_body_asks_the_expected_question(self):
        self.es.search_result = page([], total=0)

        etp.genericIntervalSearch(self.es, "reads", query="site:Namur", sort=[{"@timestamp": "desc"}])

        _, body, kwargs = self.es.searches[0]
        self.assertEqual(kwargs["size"], 10000)
        self.assertEqual(kwargs["scroll"], "2m")
        self.assertEqual(body["query"]["bool"]["must"][0]["query_string"]["query"], "site:Namur")
        self.assertTrue(body["query"]["bool"]["must"][0]["query_string"]["analyze_wildcard"])
        self.assertEqual(body["sort"], [{"@timestamp": "desc"}])
        self.assertNotIn("range", body["query"]["bool"]["must"][0], "no range without a start")

    def test_an_interval_becomes_an_epoch_milliseconds_range(self):
        self.es.search_result = page([], total=0)
        start = datetime.datetime(2026, 11, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
        end = datetime.datetime(2026, 11, 9, 0, 0, 0, tzinfo=datetime.timezone.utc)

        etp.genericIntervalSearch(self.es, "reads", start=start, end=end)

        _, body, _ = self.es.searches[0]
        window = body["query"]["bool"]["must"][1]["range"]["@timestamp"]
        self.assertEqual(window["gte"], int(start.timestamp()) * 1000)
        self.assertEqual(window["lte"], int(end.timestamp()) * 1000)
        self.assertEqual(window["format"], "epoch_millis")

    def test_the_time_field_can_be_named(self):
        self.es.search_result = page([], total=0)
        start = datetime.datetime(2026, 11, 1, tzinfo=datetime.timezone.utc)

        etp.genericIntervalSearch(self.es, "reads", start=start, end=start, timestampfield="ts")

        _, body, _ = self.es.searches[0]
        self.assertIn("ts", body["query"]["bool"]["must"][1]["range"])

    def test_only_the_requested_source_fields_are_asked_for(self):
        self.es.search_result = page([], total=0)

        etp.genericIntervalSearch(self.es, "reads", _source=["value"])

        _, body, _ = self.es.searches[0]
        self.assertEqual(body["_source"], ["value"])

    def test_the_declared_date_columns_come_back_as_timestamps(self):
        self.es.search_result = page([hit("1", when="2026-11-09T10:30:00Z")], total=0)

        frame = etp.genericIntervalSearch(self.es, "reads", datecolumns=["when"])

        self.assertEqual(frame.loc[0, "when"].year, 2026)
        self.assertIsNotNone(frame.loc[0, "when"].tzinfo)

    def test_a_cluster_error_yields_the_rows_already_collected(self):
        # The builder catches its own failures and returns what it has: a report with a partial
        # frame is a report, an exception would lose the whole export.
        class Failing(FakeES):
            def search(self, index=None, body=None, **kwargs):
                raise ConnectionError("cluster down")

        frame = etp.genericIntervalSearch(Failing(), "reads")
        self.assertEqual(len(frame), 0)


if __name__ == "__main__":
    unittest.main()
