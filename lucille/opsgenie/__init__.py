"""OpsGenie metrics.

Analyzers built on top of the CSV export that the user downloads manually
from OpsGenie's UI. A future extension will replace the CSV step with an
API fetcher; until then, submodules here consume the same CSV that
``lucille/opsgenie_graph.py`` and ``lucille/opsgenie_alerts_chart_weeks.py``
already use.
"""
