import logging
from typing import List, Tuple

logger = logging.getLogger(__name__)

# (label, lower_hours_inclusive, upper_hours_exclusive, hex_color)
_BUCKET_DEFS: List[Tuple[str, float, float, str]] = [
    ("<4 hours",    0.0,          4.0,          "#2ecc71"),
    ("4-24 hours",  4.0,         24.0,          "#27ae60"),
    ("1-3 days",   24.0,         72.0,          "#1abc9c"),
    ("3-7 days",   72.0,        168.0,          "#3498db"),
    ("7-14 days", 168.0,        336.0,          "#f39c12"),
    ("14-30 days", 336.0,       720.0,          "#e74c3c"),
    ("30+ days",  720.0,  float("inf"),         "#c0392b"),
]

_LABELS = [b[0] for b in _BUCKET_DEFS]
_COLORS = [b[3] for b in _BUCKET_DEFS]


def assign_bucket(lead_time_hours: float) -> str:
    """Return the bucket label for the given lead time in hours."""
    for label, lo, hi, _ in _BUCKET_DEFS:
        if lo <= lead_time_hours < hi:
            return label
    return "30+ days"


def bucket_labels() -> List[str]:
    """Return ordered list of all bucket labels."""
    return list(_LABELS)


def bucket_colors() -> List[str]:
    """Return ordered list of hex colors matching bucket_labels()."""
    return list(_COLORS)
