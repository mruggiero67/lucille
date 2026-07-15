"""Metrics measuring the impact of AI-assistant use on engineering output.

Answers questions like:
  - What % of PRs are AI-touched? (weekly trend)
  - Do AI-touched PRs merge at a different rate than human-only PRs?
  - Are AI-touched PRs reverted more often?
  - Are AI-touched Jira tickets faster or slower through 'In Progress → Done'?

Wired into the Makefile as ``make ai_metrics``.
"""
