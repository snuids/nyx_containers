"""The format converter: a file on the broker, converted, and put back on the broker.

The conversion itself is a shell script (`shellscripts/convert.sh`); what belongs to the container
is the contract around it — where the file is written, which command is run, and how the result is
published back with its original name.
"""

import base64
import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from harness import FakeBroker, Harness                                   # noqa: E402

SCENARIO = Harness()
converter = SCENARIO.load("nyx_formatconverter").module


class MessageReceivedTests(unittest.TestCase):

    def setUp(self):
        self.directory = tempfile.mkdtemp()
        self.saved_cwd = os.getcwd()
        os.chdir(self.directory)
        self.broker = FakeBroker()
        converter.conn = self.broker
        self.commands = []
        self.saved_run = converter.subprocess.run
        converter.subprocess.run = self.fake_run
        self.addCleanup(self.restore)

    def restore(self):
        converter.subprocess.run = self.saved_run
        converter.conn = None
        os.chdir(self.saved_cwd)
        shutil.rmtree(self.directory)

    def fake_run(self, command, cwd=None, **kwargs):
        # Stands in for convert.sh: it must produce the target file next to the source.
        self.commands.append((command, cwd))
        source, target = command[1], command[2]
        with open("./" + source, "rb") as original:
            content = original.read()
        with open("./toconvert." + target, "wb") as handle:
            handle.write(b"converted:" + content)

    def message(self, **overrides):
        payload = {
            "originalformat": "docx",
            "targetformat": "pdf",
            "destination": "/queue/NYX_CONVERTED",
            "filename": "Contrat.docx",
            "data": base64.b64encode(b"raw docx").decode("ascii"),
        }
        payload.update(overrides)
        return json.dumps(payload)

    def test_the_converted_file_comes_back_with_its_original_name(self):
        converter.messageReceived("NYX_CONVERTER", self.message(), {})

        bodies = self.broker.sent_to("/queue/NYX_CONVERTED")
        self.assertEqual(len(bodies), 1)
        self.assertEqual(base64.b64decode(bodies[0]), b"converted:raw docx")
        self.assertEqual(self.broker.messages[0][2], {"file": "Contrat.docx"})

    def test_the_conversion_script_runs_in_its_own_directory(self):
        converter.messageReceived("NYX_CONVERTER", self.message(), {})

        command, cwd = self.commands[0]
        self.assertEqual(command, ["./convert.sh", "toconvert.docx", "pdf"])
        self.assertEqual(cwd, "./shellscripts")

    def test_the_payload_is_written_before_the_conversion(self):
        seen = {}

        def inspected(command, cwd=None, **kwargs):
            with open("./toconvert.docx", "rb") as written:
                seen["content"] = written.read()
            return self.fake_run(command, cwd)

        converter.subprocess.run = inspected
        converter.messageReceived("NYX_CONVERTER", self.message(), {})

        self.assertEqual(seen["content"], b"raw docx")

    def test_a_destination_from_the_message_is_honoured(self):
        converter.messageReceived("NYX_CONVERTER",
                                  self.message(destination="/queue/OTHER_SINK"), {})
        self.assertEqual(len(self.broker.sent_to("/queue/OTHER_SINK")), 1)


if __name__ == "__main__":
    unittest.main()
