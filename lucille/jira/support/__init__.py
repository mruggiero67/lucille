"""Shared helpers for the SUP (Engineering Support) weekly analyses.

Used by ``lucille.jira.sup_cycle_time`` and ``lucille.jira.sup_ticket_volume``.
Import submodules directly:

    from lucille.jira.support.weekly import get_date_range, classify_trend
    from lucille.jira.support.io import save_issues_csv, save_summary_txt
    from lucille.jira.support.charts import create_weekly_bar_chart
    from lucille.jira.support.cli import build_common_parser, resolve_jira_credentials
"""
