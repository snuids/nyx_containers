"""Test harness for the container scripts.

The scripts in `sources/` are written to *be* a container: everything that talks to the cluster,
the broker or Redis lives under `if __name__ == '__main__'`, so the module imports cleanly but
its functions expect the globals that block sets — `logger`, `es`, `conn`, `redisserver`. They
used to be reachable only by running the container, which is why nothing here had a test.

`load()` imports a script and hands it those globals as doubles, so a test exercises the real
function instead of a copy of its logic. The doubles record what would have left for the
cluster or the broker: the tests assert on that, not on a simulated success.
"""

import importlib
import json
import logging
import os
import sys
from pathlib import Path

SOURCES = Path(__file__).resolve().parents[1] / "sources"
if str(SOURCES) not in sys.path:
    sys.path.insert(0, str(SOURCES))

# The scripts log through a module-level `logger` that only the __main__ block creates. Warnings
# and errors still go to the unittest output, which is what a failing test needs to show.
logging.basicConfig(level=logging.CRITICAL)


class ModuleUnderTest:
    """A script plus the globals it reads, restored after the test."""

    def __init__(self, module, saved):
        self.module = module
        self._saved = saved

    def restore(self):
        for name, value in self._saved.items():
            setattr(self.module, name, value)


class Harness:
    """Imports the scripts and installs their doubles, restoring `state` conventions per test."""

    def load(self, name):
        module = importlib.import_module(name)
        saved = {}
        for attribute in ("logger", "es", "conn", "redisserver", "path", "client"):
            saved[attribute] = getattr(module, attribute, _MISSING)
        if saved["logger"] is _MISSING:
            module.logger = logging.getLogger(name)
            saved["logger"] = _MISSING
        return ModuleUnderTest(module, {k: v for k, v in saved.items() if v is not _MISSING})


_MISSING = object()


class FakeBroker:
    """Double of the AMQP client: what would be published, and to which destination."""

    def __init__(self, fail=False):
        self.messages = []
        self.life_signs = []
        self.fail = fail

    def send_message(self, destination, body, headers=None):
        if self.fail:
            raise RuntimeError("broker unreachable")
        self.messages.append((destination, body, headers))
        return True

    def send_life_sign(self, variables=None):
        self.life_signs.append(variables)
        return True

    def sent_to(self, destination):
        return [body for dest, body, _ in self.messages if dest == destination]

    def last_body(self):
        return json.loads(self.messages[-1][1]) if self.messages else None


class FakeIndex:
    """Double of `es.indices`: creation recorded, existence decided by the test."""

    def __init__(self, existing=(), fail_create=False):
        self.existing = set(existing)
        self.created = []
        self.fail_create = fail_create

    def get(self, index=None, **kwargs):
        if index in self.existing:
            return {index: {"mappings": {}}}
        raise LookupError(index)

    def create(self, index=None, body=None, **kwargs):
        if self.fail_create:
            raise RuntimeError("create refused")
        self.created.append((index, body))
        self.existing.add(index)
        return {"acknowledged": True}


class FakeES:
    """Double of the Elasticsearch client, for the calls these scripts actually make."""

    def __init__(self, documents=None, search_result=None, version="8.19.23"):
        self.documents = documents or {}
        self.search_result = search_result
        self.version = version
        self.bulk_calls = []
        self.bulk_result = {"errors": False, "items": []}
        self.gets = []
        self.searches = []
        self.scrolls = []
        self.scroll_pages = []
        self.indices = FakeIndex()

    def info(self, **kwargs):
        return {"version": {"number": self.version}}

    def get(self, index=None, id=None, doc_type=None, **kwargs):
        self.gets.append((index, id))
        if id in self.documents:
            return {"_id": id, "_index": index, "_source": self.documents[id]}
        raise LookupError("%s/%s" % (index, id))

    def search(self, index=None, body=None, doc_type=None, **kwargs):
        self.searches.append((index, body, kwargs))
        return self.search_result or {"hits": {"total": 0, "hits": []}}

    def scroll(self, scroll_id=None, scroll=None, **kwargs):
        self.scrolls.append(scroll_id)
        if not self.scroll_pages:
            return {"_scroll_id": scroll_id, "hits": {"total": 0, "hits": []}}
        return self.scroll_pages.pop(0)

    def bulk(self, body=None, index=None, **kwargs):
        self.bulk_calls.append((body, kwargs))
        return self.bulk_result

    def bulk_documents(self):
        """The `_source` of every document in the bodies sent to `bulk`, in order."""
        documents = []
        for body, _ in self.bulk_calls:
            lines = [line for line in body.split("\r\n") if line.strip()]
            for position in range(1, len(lines), 2):
                documents.append(json.loads(lines[position]))
        return documents

    def bulk_actions(self):
        actions = []
        for body, _ in self.bulk_calls:
            lines = [line for line in body.split("\r\n") if line.strip()]
            for position in range(0, len(lines), 2):
                actions.append(json.loads(lines[position]))
        return actions


class FakeRedis:
    """Double of redis-py: the scripts read bytes back, so this returns bytes."""

    def __init__(self, store=None):
        self.store = dict(store or {})
        self.sets = []

    def get(self, key):
        value = self.store.get(key)
        return value if isinstance(value, bytes) or value is None else str(value).encode()

    def set(self, key, value, ex=None, *args, **kwargs):
        self.sets.append((key, ex))
        self.store[key] = value
        return True


def exists(path):
    return os.path.exists(str(path))
