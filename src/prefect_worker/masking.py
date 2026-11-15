"""Keep secrets out of query results.

Prefect variables are plain JSON values, not secrets, but teams still park
passwords, DSNs and webhook URLs in them. The worker therefore masks
independently of the server:

* ``variables`` always masks values.
* ``variable_values(reveal := true)`` shows a value only when its name does not
  look secret *and* the value does not look like a credential.
* Block documents are listed by name and type only; their ``data`` is never read
  (the worker asks for ``include_secrets: false`` and drops the field).
"""

from __future__ import annotations

import json
import re
from typing import Any

MASK = "***"

# Common credential words. Deliberately broad:
# a false positive hides a harmless value, a false negative leaks a secret.
_SENSITIVE_WORDS = (
    "access_token",
    "api_key",
    "apikey",
    "auth",
    "bearer",
    "cert",
    "conn",
    "cookie",
    "credential",
    "dsn",
    "jwt",
    "key",
    "pass",
    "private",
    "sas",
    "secret",
    "session",
    "signature",
    "token",
    "service_account",
    "keyfile",
)

# Values that look like credentials regardless of their key.
_SECRET_VALUE = re.compile(
    r"^(?:xox[abposr]-|gh[pousr]_|github_pat_|AKIA|ASIA|sk-|sk_live_|rk_live_|glpat-|ya29\.|AIza)"
    r"|^eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\."  # JWT
    r"|-----BEGIN [A-Z ]*PRIVATE KEY-----"
    r"|://[^/\s:@]+:[^/\s@]+@"  # URI with embedded password
)


def is_sensitive_key(key: str) -> bool:
    """Return True when a variable name name suggests a secret."""
    lowered = key.lower().replace("-", "_")
    return any(word in lowered for word in _SENSITIVE_WORDS)


def looks_secret(value: str) -> bool:
    """Return True when a value looks like a credential (token prefix, JWT, private key, URI password)."""
    return bool(_SECRET_VALUE.search(value.strip())) or value.strip() == MASK


def mask_variable(key: str, value: Any, *, reveal: bool) -> tuple[str | None, bool]:
    """Return ``(value_to_show, masked)`` for one Prefect variable."""
    if value is None:
        return None, False
    text = value if isinstance(value, str) else json.dumps(value)
    if reveal and not is_sensitive_key(key) and not looks_secret(text):
        return text, False
    return MASK, True
