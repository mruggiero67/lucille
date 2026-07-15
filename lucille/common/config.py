"""YAML config loading with optional validation.

Prior to this module, ~20 scripts each defined their own ``load_config``
function. They fell into three behavior classes on missing files:

  - ``on_missing="exit"``  — log an error and ``sys.exit(1)`` (default)
  - ``on_missing="raise"`` — let the ``FileNotFoundError`` propagate
  - ``on_missing="empty"`` — return an empty dict (useful when the config
    file is optional)

Additional validation (nested keys, required subsections) is deliberately
kept out of this helper: callers should check their own domain-specific
invariants after loading.
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from typing import Iterable, Literal, Optional, Union

import yaml

logger = logging.getLogger(__name__)

OnMissing = Literal["exit", "raise", "empty"]


def load_yaml_config(
    path: Union[str, os.PathLike, None],
    *,
    required_keys: Iterable[str] = (),
    subsection: Optional[str] = None,
    on_missing: OnMissing = "exit",
) -> dict:
    """Load a YAML file and optionally validate / project it.

    Args:
        path: Path to the YAML file. ``None`` is treated as "missing".
        required_keys: Top-level keys that must be present. Checked *after*
            ``subsection`` is applied, if given.
        subsection: If provided, return ``config[subsection]`` instead of the
            full document. Missing subsection is treated as a validation
            failure (logged and ``sys.exit(1)``).
        on_missing: How to handle a missing file. See module docstring.

    Returns:
        The parsed YAML document (or its subsection), always a dict. An empty
        or ``null`` YAML file yields ``{}``.
    """
    if path is None or not Path(path).exists():
        if on_missing == "empty":
            return {}
        if on_missing == "raise":
            raise FileNotFoundError(path)
        logger.error(f"Configuration file not found: {path}")
        sys.exit(1)

    try:
        with open(path, "r") as fh:
            config = yaml.safe_load(fh) or {}
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML configuration ({path}): {e}")
        sys.exit(1)

    if subsection is not None:
        if not isinstance(config, dict) or subsection not in config:
            logger.error(f"Missing required config section: {subsection!r}")
            sys.exit(1)
        config = config[subsection]

    for key in required_keys:
        if not isinstance(config, dict) or key not in config:
            logger.error(f"Missing required config key: {key!r}")
            sys.exit(1)

    return config
