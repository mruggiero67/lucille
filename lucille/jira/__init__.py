"""
Jira module for Lucille project.

This module contains various Jira-related tools and utilities:
- Epic completion analysis
- Epic filtering and extraction
- Kanban board scraping
- Label bulk updating
- Lead time baseline calculation
- Project contributor analysis
- General Jira utilities
"""

# Handle both direct script execution and module import
try:
    from .epic_completion import JiraEpicAnalyzer
    from .filter_epics import create_jira_session, get_filter_issues, extract_epic_keys
    from .kanban_scraper import JiraKanbanScraper
    from .label_updater import JiraLabelUpdater
    from .lead_time_baseline_calculator import JiraLeadTimeAnalyzer
    from .project_contributors import JiraAnalyzer
except ImportError:
    from lucille.jira.epic_completion import JiraEpicAnalyzer
    from lucille.jira.filter_epics import create_jira_session, get_filter_issues, extract_epic_keys
    from lucille.jira.kanban_scraper import JiraKanbanScraper
    from lucille.jira.label_updater import JiraLabelUpdater
    from lucille.jira.lead_time_baseline_calculator import JiraLeadTimeAnalyzer
    from lucille.jira.project_contributors import JiraAnalyzer

__all__ = [
    'JiraEpicAnalyzer',
    'JiraKanbanScraper',
    'JiraLabelUpdater',
    'JiraLeadTimeAnalyzer',
    'JiraAnalyzer',
    'create_jira_session',
    'get_filter_issues',
    'extract_epic_keys',
    'fetch_all_issues',
]
