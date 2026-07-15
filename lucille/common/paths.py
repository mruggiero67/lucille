"""Canonical filesystem locations used by lucille scripts."""

from pathlib import Path

HOME = Path.home()

# Personal "scratch" output directory. Every make target writes here (or a
# subdirectory).
DEBRIS_DIR = HOME / "Desktop" / "debris"

# 2x2 review artifacts (deployments, opsgenie, lead_time, support, publish).
TWO_X_TWO_DIR = DEBRIS_DIR / "2x2"

# Config files live outside the repo, in ~/bin, by convention.
BIN_DIR = HOME / "bin"
