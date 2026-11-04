"""Redaction of job messages before they reach a log.

A report job carries the requesting user's session token, that user's phone number, and the report
definition, which may hold a `jdbc_password` for a SQL datasource. Logging any of it verbatim wrote
a live credential into the worker logs and into `nyx_reportlog`; the runners, the lambda, the
scheduler and the report builder all log through this module so the rule lives in one place.

Mask by key name at any depth rather than by position: the secret moved between a top-level `creds`
and a nested `report` in the same year.
"""

SENSITIVE_KEY_MARKERS = ("password", "token", "secret")
SENSITIVE_KEYS = ("phone",)


def _is_sensitive_key(key):
    lowered = str(key).lower()
    return lowered in SENSITIVE_KEYS or any(marker in lowered for marker in SENSITIVE_KEY_MARKERS)


def redact_message(message):
    """Return a copy of `message` that is safe to log, wherever the secret sits."""
    def walk(node):
        if isinstance(node, dict):
            return {key: ("***" if _is_sensitive_key(key) else walk(value))
                    for key, value in node.items()}
        if isinstance(node, list):
            return [walk(item) for item in node]
        return node

    try:
        import json
        return walk(json.loads(json.dumps(message, default=str)))
    except Exception:
        return "<message not serialisable>"
