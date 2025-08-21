DATE_PREFIX := $(shell date +"%Y_%m_%d")

deploy_graph: deploy_csv
	python lucille/deployment_graph.py -c ~/bin/graphs.yaml -f ~/Desktop/debris/deployment_analysis.csv

deploy_csv:
	python lucille/slack_deploys.py -l ~/Desktop/debris/slack_deploy_log.txt -d /Users/michael@jaris.io/Desktop/debris

opsgenie:
	python lucille/opsgenie_graph.py -c ~/bin/graphs.yaml -f ~/Desktop/debris/$(DATE_PREFIX)_opsgenie.csv

wip_epics:
	python lucille/jira/filter_epics.py ~/bin/jira_epic_config.yaml

epic_completion: wip_epics
	python lucille/jira/epic_completion.py ~/bin/jira_epic_config.yaml

prs:
	python lucille/github_pr_analyzer.py ~/bin/github_config.yaml
