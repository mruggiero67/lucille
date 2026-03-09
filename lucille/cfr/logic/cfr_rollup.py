#!/usr/bin/env python3
"""
Aggregates deployment records into CFR metrics, split by category.
"""

import sys
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    from .deployment_detector import DeploymentEvent
    from .pr_classifier import classify_deployment, Category
    from .intervention_detector import InterventionResult
except ImportError:
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    from lucille.cfr.logic.deployment_detector import DeploymentEvent
    from lucille.cfr.logic.pr_classifier import classify_deployment, Category
    from lucille.cfr.logic.intervention_detector import InterventionResult

LOW_CONFIDENCE_THRESHOLD = 5


@dataclass
class DeploymentRecord:
    event: DeploymentEvent
    category: str
    intervention: InterventionResult


@dataclass
class CFRResult:
    period_start: date
    period_end: date
    repo: Optional[str]         # None = cross-repo aggregate
    total_deployments: int
    failed_deployments: int
    cfr: float                  # 0.0–1.0
    by_category: Dict[str, "CFRResult"] = field(default_factory=dict)
    low_confidence: bool = False

    @property
    def cfr_pct(self) -> str:
        return f"{self.cfr * 100:.1f}%"

    @property
    def cfr_fraction(self) -> str:
        return f"{self.failed_deployments}/{self.total_deployments}"


def compute_cfr(
    records: List[DeploymentRecord],
    period_start: date,
    period_end: date,
    repo: Optional[str] = None,
) -> CFRResult:
    """
    Compute overall CFR and a per-category breakdown from a list of records.
    Categories with fewer than LOW_CONFIDENCE_THRESHOLD deployments are flagged.
    """
    total = len(records)
    failed = sum(1 for r in records if r.intervention.detected)
    cfr_val = failed / total if total else 0.0

    by_category: Dict[str, CFRResult] = {}
    for cat in ("agent", "human", "hybrid"):
        cat_records = [r for r in records if r.category == cat]
        cat_total = len(cat_records)
        cat_failed = sum(1 for r in cat_records if r.intervention.detected)
        by_category[cat] = CFRResult(
            period_start=period_start,
            period_end=period_end,
            repo=repo,
            total_deployments=cat_total,
            failed_deployments=cat_failed,
            cfr=cat_failed / cat_total if cat_total else 0.0,
            low_confidence=cat_total < LOW_CONFIDENCE_THRESHOLD,
        )

    return CFRResult(
        period_start=period_start,
        period_end=period_end,
        repo=repo,
        total_deployments=total,
        failed_deployments=failed,
        cfr=cfr_val,
        by_category=by_category,
        low_confidence=total < LOW_CONFIDENCE_THRESHOLD,
    )
