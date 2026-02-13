DATE_PREFIX := $(shell date +"%Y_%m_%d")
7_DAYS_AGO := $(shell date -v-7d +"%Y-%m-%d")
90_DAYS_AGO := $(shell date -v-90d +"%Y-%m-%d")
TODAY := $(shell date +"%Y-%m-%d")
2X2_DIR := ~/Desktop/debris/2x2

deployments: deploy_csv
	python lucille/deployment_graph.py -c ~/bin/graphs.yaml -f ~/Desktop/debris/$(DATE_PREFIX)_deployment_analysis.csv
	python lucille/weekly_deployment_trends.py --csv ~/Desktop/debris/$(DATE_PREFIX)_deployment_analysis.csv --output-dir $(2X2_DIR)/deployments

deploy_csv:
	python lucille/slack_deploys.py -l ~/Desktop/debris/slack_deploy_log.txt -d /Users/michael@jaris.io/Desktop/debris

opsgenie:
	python lucille/opsgenie_graph.py -c ~/bin/graphs.yaml -f ~/Desktop/debris/$(DATE_PREFIX)_opsgenie.csv
	python lucille/opsgenie_alerts_chart_weeks.py --csv ~/Desktop/debris/$(DATE_PREFIX)_opsgenie.csv --weeks 6 --output $(2X2_DIR)/opsgenie/$(DATE_PREFIX)_opsgenie_alerts_last_6_weeks.png

wip_epics:
	python lucille/jira/filter_epics.py ~/bin/jira_epic_config.yaml

epic_completion: active_sprints
	python lucille/jira/epic_completion.py ~/bin/jira_epic_config.yaml

prs:
	python lucille/github/pr_analyzer.py ~/bin/github_config.yaml

active_sprints:
	python lucille/jira/active_sprints.py ~/bin/jira_epic_config.yaml

slack_deploy_log:
	python lucille/reformat_slack_scrape.py ~/Desktop/debris/raw_slack_deploy_log.txt >> ~/Desktop/debris/slack_deploy_log.txt

mv_opsgenie:
	mv ~/Downloads/finalAlertData.csv ~/Desktop/debris/$(DATE_PREFIX)_opsgenie.csv

oot_cycle_time:
	python lucille/jira/jira_cycle_time_analysis.py OOT $(90_DAYS_AGO) $(TODAY) --c ~/bin/jira_epic_config.yaml --o ~/Desktop/debris/

github_security:
	python lucille/github/fetch_github_security_alerts.py --config ~/bin/github_config.yaml

clean_2x2: ## Archive CSVs, TXTs, and PNGs to ~/Desktop/debris/2x2/archive
	@mkdir -p ~/Desktop/debris/2x2/archive && find $(2X2_DIR) -type f \( -name "*.csv" -o -name "*.txt" -o -name "*.png" \) -exec mv {} $(2X2_DIR)/archive/ \;

publish:
	python lucille/publish.py --output-dir $(2X2_DIR) --config ~/bin/jira_epic_config.yaml

2x2: deployments prs opsgenie github_security publish

.PHONY: list

list:
	@echo "Available targets:"
	@$(MAKE) -pRrq -f $(firstword $(MAKEFILE_LIST)) : 2>/dev/null | \
		awk -v RS= -F: '/(^|\n)# Files(\n|$$)/,/(^|\n)# Finished Make data base/ { \
		if ($$1 !~ "^[#.]") { \
		print $$1 \
		} \
		}' | grep -E -v -e '^[^[:alnum:]]' -e '^$$@$$' | sort
