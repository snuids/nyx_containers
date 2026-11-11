"""Redaction: what the container logs may contain.

Every runner, the lambda, the scheduler and the report builder log the job message they receive.
That message carries the requesting user's session token, their phone number and the report
definition, which may hold a `jdbc_password`. This module is the only thing between those
secrets and `docker logs` / `nyx_reportlog`, so it is worth more than a smoke test.
"""

import datetime
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from harness import SOURCES                                                    # noqa: E402,F401
from lib.redact import redact_message, _is_sensitive_key                       # noqa: E402


class RedactMessageTests(unittest.TestCase):

    def test_a_token_and_a_phone_are_masked_wherever_they_sit(self):
        message = {
            "creds": {
                "token": "6f1e9c1a-live-token",
                "user": {"id": "someone@equans.com", "phone": "0032497441962",
                         "language": "fr", "privileges": ["admin"]},
            },
            "report": {"title": "Monthly", "parameters": [{"name": "ds", "jdbc_password": "s3cret"}]},
            "task": {"mailingList": ["someone@equans.com"]},
        }

        redacted = redact_message(message)

        self.assertEqual(redacted["creds"]["token"], "***")
        self.assertEqual(redacted["creds"]["user"]["phone"], "***")
        self.assertEqual(redacted["report"]["parameters"][0]["jdbc_password"], "***")
        # The rest survives: a log line with everything masked would be useless.
        self.assertEqual(redacted["creds"]["user"]["id"], "someone@equans.com")
        self.assertEqual(redacted["creds"]["user"]["privileges"], ["admin"])
        self.assertEqual(redacted["report"]["title"], "Monthly")
        self.assertEqual(redacted["task"]["mailingList"], ["someone@equans.com"])

    def test_the_job_message_is_not_mutated(self):
        message = {"creds": {"token": "abc"}}
        redact_message(message)
        self.assertEqual(message["creds"]["token"], "abc",
                         "logging must not change the job it describes")

    def test_the_match_is_on_the_key_name_not_the_position(self):
        # The secret moved from a top-level `creds` to a nested `report` during 2026: a rule
        # keyed on the position would have stopped masking it without a test noticing.
        self.assertTrue(_is_sensitive_key("token"))
        self.assertTrue(_is_sensitive_key("JDBC_Password"))
        self.assertTrue(_is_sensitive_key("clientSecret"))
        self.assertTrue(_is_sensitive_key("phone"))
        self.assertFalse(_is_sensitive_key("privileges"))
        self.assertFalse(_is_sensitive_key("phoneme"))

    def test_a_list_of_parameters_is_walked(self):
        redacted = redact_message({"parameters": [{"name": "a", "token": "t"},
                                                  {"name": "b", "nested": {"secret": "s"}}]})
        self.assertEqual(redacted["parameters"][0]["token"], "***")
        self.assertEqual(redacted["parameters"][1]["nested"]["secret"], "***")
        self.assertEqual(redacted["parameters"][1]["name"], "b")

    def test_datetimes_survive_as_strings(self):
        # json.dumps(default=str) is what makes a report definition serialisable at all.
        redacted = redact_message({"nextRun": datetime.datetime(2026, 11, 9, 10, 30),
                                   "interval": datetime.time(7, 15)})
        self.assertIn("2026-11-09", redacted["nextRun"])
        self.assertEqual(redacted["interval"], "07:15:00")

    def test_a_message_that_cannot_be_serialised_says_so(self):
        class Hostile(object):
            def __str__(self):
                raise ValueError("no")

        self.assertEqual(redact_message({"bad": Hostile()}), "<message not serialisable>")

    def test_a_plain_string_passes_through(self):
        self.assertEqual(redact_message("hello"), "hello")

    def test_a_token_inside_a_url_is_masked(self):
        # The Jasper call receives the datasource URL, token included, as a plain value: a rule that
        # only looks at key names lets it through.
        redacted = redact_message({"jsonUrl": "https://host/api/v1/datasource/sites?token=abc&flat=true"})

        self.assertEqual(redacted["jsonUrl"],
                         "https://host/api/v1/datasource/sites?token=***&flat=true")

    def test_a_password_inside_a_jdbc_url_is_masked(self):
        value = "jdbc:sqlserver://db:1433;databaseName=nyx;password=S3cret!;user=nyx"

        self.assertEqual(redact_message({"jdbc_url": value})["jdbc_url"],
                         "jdbc:sqlserver://db:1433;databaseName=nyx;password=***;user=nyx")

    def test_an_ordinary_url_is_left_alone(self):
        url = "https://host/generatedreports/id_1_2.xlsx"

        self.assertEqual(redact_message({"downloads": [{"url": url}]})["downloads"][0]["url"], url)


if __name__ == "__main__":
    unittest.main()
