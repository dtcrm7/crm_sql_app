"""Centralized sheet dropdown values and normalization maps.

Update values here once to keep sheet UI options and sync parsing aligned.
"""

from __future__ import annotations


def _alias_map(canonical_values: list[str], extra_aliases: dict[str, str] | None = None) -> dict[str, str]:
    mapping = {v.lower(): v for v in canonical_values}
    if extra_aliases:
        mapping.update({k.lower(): v for k, v in extra_aliases.items()})
    return mapping


# BD sheet values
BD_CALL_STATUS_VALUES = [
    "Connected",
    "Did not connect",
    "Call back later",
    "Do not Disturb",
    "Invalid Number",
    "Referred",
]

BD_CURRENT_STATE_VALUES = [
    "Interested",
    "Rescheduled",
    "Attempt Again",
    "Attempt Again after 3 months",
    "Shared Story",
    "Snapshot Sent",
    "Not interested",
    "Do not Disturb",
    "Allocate Again",
]

BD_CALL_STATUS_ALIASES = _alias_map(
    BD_CALL_STATUS_VALUES,
    {
        "invalid number": "Invalid Number",
        "do not disturb": "Do not Disturb",
        "call back later": "Call back later",
    },
)

BD_CURRENT_STATE_ALIASES = _alias_map(
    BD_CURRENT_STATE_VALUES,
    {
        "attempt after 3 months": "Attempt Again after 3 months",
        "do not disturb.": "Do not Disturb",
        "dream snapshot sent": "Snapshot Sent",
    },
)

BD_FINAL_CLOSE_STATUSES = {"Do not Disturb", "Referred"}
BD_STOP_FOLLOWUP_STATES = {
    "Attempt Again after 3 months",
    "Allocate Again",
    "Not interested",
    "Do not Disturb",
    "Snapshot Sent",
}


# MQL sheet values
MQL_LEAD_CATEGORY_VALUES = ["Hot", "Warm", "Cold"]

MQL_CALL_STATUS_VALUES = [
    "Connected",
    "Did not connect",
    "Call back later",
    "Do not Disturb",
    "Invalid Number",
    "Referred",
]

MQL_CURRENT_STATE_VALUES_IN_PROGRESS = [
    "Escalate",
    "Attempt Again",
    "Rescheduled",
    "Respondent",
    "Dream Snapshot Confirmed",
    "Allocate Again 3 months",
    "Interested",
    "Snapshot Sent",
    "Snapshot Confirmed",
    "Meeting Requested",
]

MQL_CURRENT_STATE_VALUES_MEETING = [
    "Meeting Scheduled",
    "Meeting Held",
    "Solution Sent",
]

MQL_CURRENT_STATE_VALUES_CLOSING = [
    "Solution Picked",
    "Picked Solution",
    "Not interested",
    "Do not Disturb",
    "Reffered",
    "Irrelevant",
]

MQL_CURRENT_STATE_VALUES = (
    MQL_CURRENT_STATE_VALUES_IN_PROGRESS
    + MQL_CURRENT_STATE_VALUES_MEETING
    + MQL_CURRENT_STATE_VALUES_CLOSING
)

MQL_MESSAGE_STATUS_VALUES = ["Yes", "No"]

MQL_CALL_STATUS_ALIASES = _alias_map(
    MQL_CALL_STATUS_VALUES,
    {
        "invalid number": "Invalid Number",
        "do not disturb": "Do not Disturb",
        "call back later": "Call back later",
    },
)

MQL_CURRENT_STATE_ALIASES = _alias_map(
    MQL_CURRENT_STATE_VALUES,
    {
        "shared story": "Shared Story",
        "attempt again after 3 months": "Attempt Again after 3 months",
        "allocate again": "Allocate Again",
        "referred": "Reffered",
    },
)

MQL_CLOSE_QUALIFIED_STATES = {"Picked Solution", "Solution Picked"}
MQL_CLOSE_REJECTED_STATES = {"Not interested", "Do not Disturb", "Reffered", "Irrelevant"}
