"""The report runner's notification: what the user sees in the console when a report ends.

The runner pushes the result on `/topic/TO_UI`, which the WebSocket gateway relays to the open
console. The message is built from the job message, so a missing field used to mean a silent
report — no notification at all, and nothing in the log either.
"""

import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from harness import FakeBroker, Harness                                   # noqa: E402

SCENARIO = Harness()
runner = SCENARIO.load("nyx_reportrunner").module


def job(**overrides):
    message = {
        "id": "id_42",
        "creds": {"user": {"id": "alice@equans.com", "login": "alice", "user": "alice",
                           "language": "fr"}},
        "report": {"title": "Monthly energy", "reportType": "kpi"},
        "treatment": {"status": "Finished"},
        "downloads": ["https://energy.marmar.ovh/generatedreports/42.xlsx"],
    }
    message.update(overrides)
    return message


class NotifyReportFinishedTests(unittest.TestCase):

    def setUp(self):
        self.broker = FakeBroker()
        runner.conn = self.broker
        self.addCleanup(setattr, runner, "conn", None)

    def sent(self):
        bodies = self.broker.sent_to("/topic/TO_UI")
        self.assertEqual(len(bodies), 1, "one notification, on the interface topic")
        return json.loads(bodies[0])

    def test_a_finished_report_reaches_the_asking_user(self):
        runner.notifyReportFinished(job())

        notification = self.sent()
        self.assertEqual(notification["login"], "alice")
        data = notification["data"]["data"]
        self.assertEqual(data["notif_title"], "Rapport prêt")
        self.assertEqual(data["notif_message"], "Monthly energy")
        self.assertEqual(data["notif_message_type"], "success")
        self.assertEqual(data["notif_code"], "report_finished")
        self.assertEqual(data["report_id"], "id_42")
        self.assertEqual(data["downloads"], ["https://energy.marmar.ovh/generatedreports/42.xlsx"])

    def test_a_failed_report_says_why(self):
        runner.notifyReportFinished(job(treatment={"status": "Failed", "error": "jasper timed out"}))

        data = self.sent()["data"]["data"]
        self.assertEqual(data["notif_title"], "Échec du rapport")
        self.assertEqual(data["notif_message"], "Monthly energy: jasper timed out")
        self.assertEqual(data["notif_message_type"], "error")
        self.assertEqual(data["report_status"], "Failed")

    def test_an_unknown_error_is_still_a_message(self):
        runner.notifyReportFinished(job(treatment={"status": "Failed"}))
        self.assertEqual(self.sent()["data"]["data"]["notif_message"],
                         "Monthly energy: Unknown error")

    def test_a_long_error_is_truncated(self):
        runner.notifyReportFinished(job(treatment={"status": "Failed", "error": "x" * 500}))
        message = self.sent()["data"]["data"]["notif_message"]
        self.assertEqual(len(message), len("Monthly energy: ") + 300)

    def test_the_language_of_the_account_is_used(self):
        message = job()
        message["creds"]["user"]["language"] = "el"
        runner.notifyReportFinished(message)
        self.assertEqual(self.sent()["data"]["data"]["notif_title"], "Η αναφορά είναι έτοιμη")

    def test_an_unknown_language_falls_back_to_english(self):
        message = job()
        message["creds"]["user"]["language"] = "nl"
        runner.notifyReportFinished(message)
        self.assertEqual(self.sent()["data"]["data"]["notif_title"], "Report ready")

    def test_the_scheduler_is_not_notified(self):
        # A periodic report is requested by the scheduler, not by a person: there is nobody to
        # tell, and it used to send a notification to a login that does not exist.
        message = job()
        message["creds"]["user"] = {"user": "ReportScheduler", "login": "ReportScheduler",
                                    "language": "en"}
        runner.notifyReportFinished(message)
        self.assertEqual(self.broker.messages, [])

    def test_a_message_without_a_user_is_ignored(self):
        runner.notifyReportFinished(job(creds={}))
        self.assertEqual(self.broker.messages, [])

    def test_an_unreachable_broker_does_not_lose_the_report(self):
        runner.conn = FakeBroker(fail=True)
        runner.notifyReportFinished(job())      # must not raise: the file is already generated


if __name__ == "__main__":
    unittest.main()
