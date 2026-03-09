#!/usr/bin/env python3
"""
Writes per-deployment CFR detail rows to CSV.
"""

import csv
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List

try:
    from ..logic.cfr_rollup import DeploymentRecord
    from ..logic.pr_classifier import is_agent_pr
except ImportError:
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    from lucille.cfr.logic.cfr_rollup import DeploymentRecord
    from lucille.cfr.logic.pr_classifier import is_agent_pr

logger = logging.getLogger(__name__)

COLUMNS = [
    "deployment_id",
    "repo",
    "timestamp",
    "category",
    "pr_count",
    "agent_pr_count",
    "intervention_detected",
    "intervention_reason",
    "confidence",
    "evidence_prs",
    "evidence_jira",
]


def write_csv(
    records: List[DeploymentRecord],
    output_path: Path,
    config: Dict[str, Any],
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        for rec in records:
            agent_count = sum(1 for pr in rec.event.prs if is_agent_pr(pr, config))
            writer.writerow(
                {
                    "deployment_id": rec.event.deployment_id,
                    "repo": rec.event.repo,
                    "timestamp": rec.event.timestamp.isoformat(),
                    "category": rec.category,
                    "pr_count": len(rec.event.prs),
                    "agent_pr_count": agent_count,
                    "intervention_detected": rec.intervention.detected,
                    "intervention_reason": rec.intervention.reason or "",
                    "confidence": rec.intervention.confidence if rec.intervention.detected else "n/a",
                    "evidence_prs": ", ".join(rec.intervention.evidence_prs),
                    "evidence_jira": ", ".join(rec.intervention.evidence_jira),
                }
            )
    logger.info(f"CFR detail CSV written to {output_path}")
