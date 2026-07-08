"""disponent — dispatch work to coding agents, in Python.

The engine (dispatch/sessions/events/send/cancel/resume/reap/reconcile/
driverPlan, plus the blocking wait) is the compiled `_disponent` extension;
this package re-exports its surface.
"""
from ._disponent import (  # noqa: F401
    Disponent,
    EnvKind,
    EventKind,
    ExitReason,
    Fidelity,
    IsolationKind,
    SessionState,
)

__all__ = [
    "Disponent",
    "EnvKind",
    "EventKind",
    "ExitReason",
    "Fidelity",
    "IsolationKind",
    "SessionState",
]
