#!/bin/sh
# Runs the nyx_containers suite under coverage, inside the image the CI has just built.
# POSIX sh: the base image is python:3.11-alpine, without bash.
#
# Two lies this script stops, the same two as in nyx_rest:
#   * `unittest` exits 0 even when it skips tests, and a test that skips itself for a missing
#     dependency disables itself in silence;
#   * nothing measured how much of these scripts the tests actually exercise.
#
# The floor is the level actually measured, never a wish: 29 % on 2026-11-09, when the repository
# went from no test at all to covering redaction, the bulk writer, the scheduler's date formulas
# and cron, the lambda's notebook writer and reload detection, the importer, the converter, the
# docker monitor and the runner's notifications; then 34 % the same day once the scheduler's
# `checkTasks` loop was held (`nyx_reportscheduler.py` 52 % -> 70 %), the two helpers no script
# imported were deleted (147 statements) and the example skeleton left the measure. Raise it as
# tests land, never lower it.
set -eu

FLOOR=34

python -m pip install --quiet -r tests/requirements-test.txt
python -m coverage erase

# No pipeline here: we want unittest's exit status, not tee's.
status=0
python -m coverage run --source=sources -m unittest discover -s tests > /tmp/suite.log 2>&1 || status=$?
cat /tmp/suite.log

if grep -q 'skipped=' /tmp/suite.log; then
    echo '::error::Tests were skipped although the dependencies are present: this is a regression, not an incomplete environment.'
    exit 1
fi

if [ "$status" -ne 0 ]; then
    exit "$status"
fi

python -m coverage report --fail-under="$FLOOR"

# The percentage, alone, for the badge the README points at. Written where the workflow can
# read it: the repository is mounted at /work.
python -m coverage report --format=total > .coverage-total
