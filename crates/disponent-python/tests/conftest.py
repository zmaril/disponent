"""Shared setup for the python-binding tests.

The dry-run backend flags must be in the process environment BEFORE the engine
is constructed. CPython's ``os.environ`` writes through to the native
environment (unlike Bun, which snapshots), so setting them here — at import,
before any test builds a ``Disponent`` — is enough; no native ``set_env`` shim
is needed.
"""

import os

os.environ.setdefault("DISPONENT_LOCAL_DRY_RUN", "1")
os.environ.setdefault("DISPONENT_EXE_DRY_RUN", "1")
