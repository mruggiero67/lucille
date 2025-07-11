deploy_graph: deploy_csv
	python lucille/deployment_graph.py -c ~/bin/deployments.yaml -f ~/Desktop/debris/deployment_analysis.csv

deploy_csv:
	python lucille/slack_deploys.py -l ~/Desktop/debris/slack_deploy_log.txt -d /Users/michael@jaris.io/Desktop/debris
