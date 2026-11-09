"""The report runner's job: `messageReceivedReport`, from step 1 to step 3.

The runner receives the job on `/queue/NYX_REPORT_STEP1`, decides what kind of report it is
(jasper, a python script, a notebook, a docx), runs it, checks the files it should have produced,
and sends the annotated job on `/queue/NYX_REPORT_STEP3` — always, success or failure, because the
step 3 is what closes the job and notifies the user.

Everything the runner touches is doubled here: the broker, `subprocess` (which stands in for the
builders and writes the files they declare) and `requests` (the Jasper generator API). The
assertions are on what would have been run and published.
"""

import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

from harness import FakeBroker, Harness                                   # noqa: E402

SCENARIO = Harness()
runner = SCENARIO.load("nyx_reportrunner").module

REPORT_URL = "https://energy.marmar.ovh/generatedreports"


class FakeProcess:
    def __init__(self, command, cwd=None, returncode=0, output=b"builder output\n"):
        self.command = command
        self.cwd = cwd
        self.returncode = returncode
        self.output = output

    def communicate(self):
        return self.output, None


class FakeSubprocess:
    """Double of the `subprocess` module.

    `Popen` stands in for the builder: it reads the job it is handed and writes the files the
    report declares, which is what the real builder does — so the runner's existence check is
    exercised against something real.
    """

    # The constants the runner passes to Popen.
    PIPE = -1
    STDOUT = -2

    def __init__(self, creates_outputs=True, returncode=0):
        self.popen = []
        self.runs = []
        self.creates_outputs = creates_outputs
        self.returncode = returncode

    def Popen(self, command, cwd=None, stdout=None, stderr=None, **kwargs):
        self.popen.append((command, cwd))
        if self.creates_outputs and len(command) > 2:
            job = json.loads(command[2])
            for extension in job["report"]["output"]:
                path = job["output"] + "." + extension
                directory = os.path.dirname(path)
                if directory and not os.path.isdir(directory):
                    os.makedirs(directory)
                with open(path, "w") as handle:
                    handle.write("report")
        return FakeProcess(command, cwd, self.returncode)

    def run(self, command, cwd=None, **kwargs):
        self.runs.append((command, cwd))
        return FakeProcess(command, cwd)


class FakeResponse:
    def __init__(self, status_code=200, text="OK", payload=None):
        self.status_code = status_code
        self.text = text
        self._payload = payload if payload is not None else {}

    def json(self):
        return self._payload


class FakeRequests:
    """Double of the `requests` module: what was posted to the Jasper generator."""

    def __init__(self, response=None):
        self.response = response or FakeResponse()
        self.posts = []

    def post(self, url, json=None, **kwargs):
        self.posts.append((url, json))
        return self.response


class ReportJobTests(unittest.TestCase):

    def setUp(self):
        self.directory = tempfile.mkdtemp()
        self.saved_cwd = os.getcwd()
        os.chdir(self.directory)
        os.makedirs("generated")

        self.broker = FakeBroker()
        runner.conn = self.broker
        self.subprocess = FakeSubprocess()
        self.requests = FakeRequests()
        self._saved_subprocess = runner.subprocess
        self._saved_requests = runner.requests
        runner.subprocess = self.subprocess
        runner.requests = self.requests

        self.env = mock.patch.dict(os.environ, {
            "REPORT_URL": REPORT_URL,
            "JASPER_API": "http://jasper:8080/report",
            "DATA_SOURCE_API": "http://energy.marmar.ovh/api/data?name=",
        })
        self.env.start()
        self.addCleanup(self.restore)

    def restore(self):
        self.env.stop()
        runner.subprocess = self._saved_subprocess
        runner.requests = self._saved_requests
        runner.conn = None
        os.chdir(self.saved_cwd)
        shutil.rmtree(self.directory)

    # -- fixtures ---------------------------------------------------------
    def script(self, name="report1.py"):
        path = os.path.join(self.directory, name)
        with open(path, "w") as handle:
            handle.write("# the report builder\n")
        return path

    def job(self, without=(), **report_fields):
        report = {"title": "Monthly",
                  "exec": self.script(),
                  "reportType": "python",
                  "output": ["xlsx", "pdf"],
                  "generatePDF": False,
                  "parameters": []}
        report.update(report_fields)
        for field in without:
            report.pop(field, None)
        return json.dumps({
            "id": "id_42",
            "creds": {"token": "token-42",
                      "user": {"id": "alice@equans.com", "login": "alice", "user": "alice",
                               "language": "en"}},
            "report": json.dumps(report),
            "treatment": {"status": "Waiting"},
            "task": {},
        })

    def run_job(self, without=(), **report_fields):
        runner.messageReceivedReport("/queue/NYX_REPORT_STEP1",
                                     self.job(without=without, **report_fields), {})
        bodies = self.broker.sent_to("/queue/NYX_REPORT_STEP3")
        self.assertTrue(bodies, "the step 3 message is always sent, success or failure")
        return json.loads(bodies[-1])

    # -- a python report --------------------------------------------------
    def test_a_python_report_runs_its_script_and_registers_the_downloads(self):
        script = self.script()

        message = self.run_job(exec=script)

        command, cwd = self.subprocess.popen[0]
        self.assertEqual(command[0], "python3")
        self.assertEqual(command[1], "report1.py")
        self.assertEqual(cwd, self.directory, "the script runs from its own directory")
        self.assertEqual(json.loads(command[2])["id"], "id_42", "the job is passed to the builder")
        self.assertEqual(message["treatment"]["status"], "Finished")
        self.assertEqual(message["treatment"]["error"], "")
        self.assertEqual([download["extension"] for download in message["downloads"]],
                         ["xlsx", "pdf"])
        self.assertEqual([download["url"] for download in message["downloads"]],
                         [REPORT_URL + "/id_42.xlsx", REPORT_URL + "/id_42.pdf"])

    def test_the_output_folder_is_built_from_the_job_id(self):
        message = self.run_job()
        self.assertEqual(message["output"], os.path.join(self.directory, "generated", "id_42"))
        self.assertIn("duration", message["treatment"])
        self.assertIn("start", message["treatment"])
        self.assertIn("end", message["treatment"])

    def test_a_missing_script_is_refused_without_running_anything(self):
        message = self.run_job(exec=os.path.join(self.directory, "absent.py"))

        self.assertEqual(self.subprocess.popen, [])
        self.assertEqual(message["treatment"]["status"], "Error")
        self.assertIn("does not exist", message["treatment"]["error"])
        # The key is always there, empty when nothing was produced: the mail flow reads
        # `downloads[0].url`, and an absent key used to break the notification instead of telling
        # the user the report failed.
        self.assertEqual(message["downloads"], [])

    def test_a_report_without_a_script_is_refused(self):
        message = self.run_job(without=("exec",))

        self.assertEqual(self.subprocess.popen, [])
        self.assertEqual(message["treatment"]["error"], "exec not defined")
        self.assertEqual(message["downloads"], [])

    def test_a_builder_that_crashes_fails_the_job(self):
        self.subprocess.returncode = 2

        message = self.run_job()

        self.assertEqual(message["treatment"]["status"], "Error")
        self.assertIn("crashed. Return code=2", message["treatment"]["error"])

    def test_an_output_the_builder_did_not_write_fails_the_job(self):
        self.subprocess.creates_outputs = False

        message = self.run_job()

        self.assertEqual(message["treatment"]["status"], "Error")
        self.assertIn("id_42.xlsx does not exist", message["treatment"]["error"])
        self.assertEqual(message["downloads"], [])

    # -- notebooks and PDF conversion -------------------------------------
    def test_a_notebook_report_needs_its_notebook_on_disk(self):
        message = self.run_job(reportType="notebook", notebook="monthly-breakdown")
        self.assertIn("Notebook file", message["treatment"]["error"])

        os.makedirs("reports/notebooks")
        with open("reports/notebooks/monthly-breakdown.ipynb", "w") as handle:
            handle.write("{}")
        message = self.run_job(reportType="notebook", notebook="monthly-breakdown")

        self.assertEqual(self.subprocess.popen[0][0][1], "nyx_buildreport.py")
        self.assertEqual(message["treatment"]["status"], "Finished")

    def test_a_docx_report_needs_both_the_document_and_the_notebook(self):
        os.makedirs("reports/notebooks")
        with open("reports/notebooks/monthly.docx", "w") as handle:
            handle.write("doc")

        message = self.run_job(reportType="notebook_doc", notebook="monthly")
        self.assertIn("Notebook file", message["treatment"]["error"])

        with open("reports/notebooks/monthly.ipynb", "w") as handle:
            handle.write("{}")
        self.run_job(reportType="notebook_doc", notebook="monthly")
        self.assertEqual(self.subprocess.popen[-1][0][1], "nyx_buildreport_doc.py")

    def test_the_outputs_are_converted_to_pdf_when_the_report_asks(self):
        self.run_job(generatePDF=True, output=["xlsx", "pdf"])

        self.assertEqual(len(self.subprocess.runs), 1, "the pdf output is not converted again")
        command, cwd = self.subprocess.runs[0]
        self.assertEqual(command[0], "./converttopdsf.sh")
        self.assertTrue(command[1].endswith("id_42.xlsx"))
        self.assertEqual(cwd, "./shellscripts")

    # -- jasper ------------------------------------------------------------
    def test_a_jasper_report_posts_its_datasource_and_interval_to_the_generator(self):
        message = self.run_job(reportType="jasper", jasper="monthly.jrxml", datasource="sites",
                               output=[],
                               parameters=[{"name": "period", "type": "interval",
                                            "value": ["2026-11-01T00:00:00Z",
                                                      "2026-11-08T00:00:00Z"]}])

        url, body = self.requests.posts[0]
        self.assertEqual(url, "http://jasper:8080/report")
        self.assertEqual(body["template"], "monthly.jrxml")
        self.assertEqual(body["outputName"], "id_42")
        self.assertEqual(body["token"], "token-42")
        self.assertIn("name=sites?token=token-42&flat=true", body["jsonUrl"])
        self.assertIn("&start=2026-11-01T00:00:00Z&end=2026-11-08T00:00:00Z", body["jsonUrl"])
        self.assertEqual(message["treatment"]["status"], "Finished")

    def test_a_jasper_report_without_a_datasource_is_refused(self):
        message = self.run_job(reportType="jasper", jasper="monthly.jrxml")

        self.assertEqual(self.requests.posts, [])
        self.assertEqual(message["treatment"]["error"], "Datasource not defined for Jasper report")

    def test_a_jdbc_jasper_report_sends_the_connection_parameters(self):
        self.run_job(reportType="jasper_jdbc", jasper="monthly.jrxml", output=[],
                     jdbc_driver="oracle.jdbc.OracleDriver", jdbc_url="jdbc:oracle:thin:@db:1521/xe",
                     jdbc_login="nyx", jdbc_password="s3cret")

        body = self.requests.posts[0][1]
        self.assertEqual(body["jdbc_url"], "jdbc:oracle:thin:@db:1521/xe")
        self.assertEqual(body["jdbc_login"], "nyx")
        self.assertEqual(body["jdbc_password"], "s3cret")
        self.assertNotIn("jsonUrl", body)

    def test_a_jasper_report_without_a_template_is_refused(self):
        message = self.run_job(reportType="jasper_jdbc", datasource="sites", output=[])
        self.assertEqual(message["treatment"]["error"], "Jasper not defined")
        self.assertEqual(self.requests.posts, [])

    def test_a_generator_error_is_reported_with_its_message(self):
        self.requests.response = FakeResponse(status_code=500, text="boom",
                                              payload={"message": "template not found"})

        message = self.run_job(reportType="jasper", jasper="monthly.jrxml", datasource="sites",
                               output=[])

        self.assertEqual(message["treatment"]["status"], "Error")
        self.assertEqual(message["treatment"]["error"], "template not found")

    # -- what always happens ----------------------------------------------
    def test_one_step_three_message_per_job(self):
        self.run_job()
        self.assertEqual(len(self.broker.sent_to("/queue/NYX_REPORT_STEP3")), 1)

    def test_the_user_is_notified_of_the_outcome(self):
        self.run_job()

        notifications = self.broker.sent_to("/topic/TO_UI")
        self.assertEqual(len(notifications), 1)
        self.assertEqual(json.loads(notifications[0])["data"]["data"]["report_id"], "id_42")

    def test_the_job_is_logged_redacted(self):
        with self.assertLogs(level="INFO") as captured:
            self.run_job()

        output = "\n".join(captured.output)
        self.assertNotIn("token-42", output)
        self.assertIn("***", output)


if __name__ == "__main__":
    unittest.main()
