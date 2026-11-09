"""The scheduler: the date formulas, the cron expression it derives, and the job it dispatches.

`computeDefaultValue` runs `eval()` on a formula assembled from the task document, and
`computeCronOfTask` decides *when* a periodic report fires. Both are pure enough to pin down,
and both are the kind of code that breaks silently — a wrong cron means the report simply stops
arriving, with nothing in the logs.
"""

import datetime
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from harness import FakeBroker, FakeES, Harness                           # noqa: E402

SCENARIO = Harness()
scheduler = SCENARIO.load("nyx_reportscheduler").module


def local(duetime, offset_seconds):
    """What the formula yields: an epoch offset, read back in the container's zone.

    A `now-30d` that crosses a daylight-saving change lands one hour away from the same wall
    clock 30 days earlier — that is what the epoch arithmetic does, and the scheduler is right
    to work in absolute time.
    """
    return datetime.datetime.fromtimestamp(int(duetime.timestamp()) + offset_seconds)


class ComputeDefaultValueTests(unittest.TestCase):
    """`now-30d`, `now-2h`, `now-15m`: the formulas the report parameters are written with."""

    def setUp(self):
        self.duetime = datetime.datetime(2026, 11, 9, 12, 0, 0)

    def test_a_day_offset_is_subtracted(self):
        self.assertEqual(scheduler.computeDefaultValue(self.duetime, "now-30d"),
                         local(self.duetime, -30 * 24 * 3600))

    def test_an_hour_offset_is_subtracted(self):
        self.assertEqual(scheduler.computeDefaultValue(self.duetime, "now-12h"),
                         self.duetime - datetime.timedelta(hours=12))

    def test_a_minute_offset_is_subtracted(self):
        self.assertEqual(scheduler.computeDefaultValue(self.duetime, "now-15m"),
                         self.duetime - datetime.timedelta(minutes=15))

    def test_units_can_be_combined(self):
        self.assertEqual(scheduler.computeDefaultValue(self.duetime, "now-1d-30m"),
                         local(self.duetime, -(24 * 3600 + 30 * 60)))

    def test_now_is_the_due_time_itself(self):
        self.assertEqual(scheduler.computeDefaultValue(self.duetime, "now"), self.duetime)


class ResolveDateStringTests(unittest.TestCase):
    """The filename formulas: `Myfile-${DATE:%d%B%Y:now-30d}-biac`."""

    def setUp(self):
        self.adate = datetime.datetime(2026, 11, 9)

    def test_the_placeholder_is_replaced_and_the_rest_kept(self):
        resolved = scheduler.resolveDateString("Myfile-${DATE:%Y%m%d:now-30d}-biac", self.adate)
        self.assertEqual(resolved, "Myfile-20261010-biac")

    def test_a_placeholder_without_a_formula_uses_the_due_date(self):
        self.assertEqual(scheduler.resolveDateString("report-${DATE:%Y%m%d}", self.adate),
                         "report-20261109")

    def test_a_name_without_a_placeholder_is_returned_unchanged(self):
        self.assertEqual(scheduler.resolveDateString("plain-name.xlsx", self.adate),
                         "plain-name.xlsx")


class ComputeCronOfTaskTests(unittest.TestCase):

    def setUp(self):
        self.curtime = datetime.datetime(2026, 11, 9, 12, 0, 0)

    def test_a_daily_task_maps_the_ui_weekdays_to_cron(self):
        # The interface numbers the days from Monday (0); cron numbers them from Sunday.
        task = {"trigger": {"type": "daily", "time": "07:30", "days": [0, 2]}}

        cron, nextrun = scheduler.computeCronOfTask(task, self.curtime)

        self.assertEqual(cron, "30 7 * * 1,3")
        self.assertGreater(nextrun, self.curtime)

    def test_sunday_is_zero_in_cron(self):
        # (day + 1) % 7: the interface's Monday-first numbering becomes cron's Sunday-first one.
        task = {"trigger": {"type": "daily", "time": "06:00", "days": [6]}}
        cron, _ = scheduler.computeCronOfTask(task, self.curtime)
        self.assertEqual(cron, "0 6 * * 0")

    def test_a_monthly_task_uses_the_day_of_the_month(self):
        task = {"trigger": {"type": "monthly", "time": "23:15", "days": [1, 15]}}

        cron, nextrun = scheduler.computeCronOfTask(task, self.curtime)

        self.assertEqual(cron, "15 23 1,15 * *")
        self.assertGreater(nextrun, self.curtime)

    def test_the_next_run_is_the_next_occurrence_not_tomorrow(self):
        task = {"trigger": {"type": "daily", "time": "23:15", "days": [0]}}
        _, nextrun = scheduler.computeCronOfTask(task, self.curtime)
        self.assertEqual(nextrun.date(), self.curtime.date(),
                         "23:15 is still ahead at noon: the run is tonight")


class ExecuteReportTests(unittest.TestCase):
    """What leaves for the report runner once the parameters are resolved."""

    def setUp(self):
        scheduler.es = FakeES(documents={
            "monthly": {
                "title": "Monthly",
                "parameters": [
                    {"name": "period", "type": "interval", "value": "now-1d:now"},
                    {"name": "day", "type": "date", "value": "now-1d"},
                    {"name": "site", "type": "escombo", "value": "Namur"},
                ],
            },
        })
        scheduler.conn = FakeBroker()
        self.duetime = datetime.datetime(2026, 11, 9, 6, 0, 0)

    def tearDown(self):
        scheduler.es = None
        scheduler.conn = None

    def dispatched(self, task):
        scheduler.execute_report(self.duetime, task)
        body = scheduler.conn.sent_to("/queue/NYX_REPORT_STEP1")
        self.assertEqual(len(body), 1, "one job, on the step 1 queue")
        return json.loads(body[0])

    def test_the_interval_parameter_is_resolved_into_two_iso_dates(self):
        message = self.dispatched({"report": "monthly"})

        period = message["report"]["parameters"][0]
        self.assertEqual(period["value"], [(self.duetime - datetime.timedelta(days=1)).isoformat(),
                                           self.duetime.isoformat()])

    def test_the_date_parameter_is_resolved(self):
        message = self.dispatched({"report": "monthly"})
        self.assertEqual(message["report"]["parameters"][1]["value"],
                         (self.duetime - datetime.timedelta(days=1)).isoformat())

    def test_an_untouched_parameter_keeps_its_value(self):
        message = self.dispatched({"report": "monthly"})
        self.assertEqual(message["report"]["parameters"][2]["value"], "Namur")

    def test_the_task_carries_the_scheduler_identity(self):
        message = self.dispatched({"report": "monthly"})

        self.assertTrue(message["id"].startswith("id_"))
        self.assertEqual(message["creds"]["user"]["id"], "ReportScheduler")
        self.assertEqual(message["privileges"], ["admin"])
        self.assertEqual(message["creds"]["token"], "reportscheduler")

    def test_the_parameters_sent_in_the_task_override_the_report_ones(self):
        # They *replace* them wholesale rather than merging: the runner receives exactly the
        # parameters the task carries, which is how a periodic report keeps its own window.
        message = self.dispatched({"report": "monthly",
                                   "parameters": [{"name": "site", "type": "escombo",
                                                   "value": "Liège"}]})
        self.assertEqual(message["report"]["parameters"],
                         [{"name": "site", "type": "escombo", "value": "Liège"}])

    def test_the_attachment_name_goes_through_the_date_formula(self):
        message = self.dispatched({"report": "monthly",
                                   "attachmentName": "Mensuel-${DATE:%Y%m%d:now-1d}"})
        self.assertEqual(message["mailAttachmentName"], "Mensuel-20261108")

    def test_the_mail_subject_is_na_when_the_task_has_none(self):
        # `execute_report` looks for `mailSubject` on the message it builds, which never has it:
        # the subject is always "NA". Pinned so the day it is fixed, the change is deliberate.
        message = self.dispatched({"report": "monthly", "mailSubject": "Rapport mensuel"})
        self.assertEqual(message["mailSubject"], "NA")


if __name__ == "__main__":
    unittest.main()
