DATE_PREFIX := $(shell date +"%Y_%m_%d")
7_DAYS_AGO := $(shell date -v-7d +"%Y-%m-%d")
90_DAYS_AGO := $(shell date -v-90d +"%Y-%m-%d")
TODAY := $(shell date +"%Y-%m-%d")
2X2_DIR := ~/Desktop/debris/2x2

deployments:
	python lucille/github/deploy_history.py --output-dir $(2X2_DIR)/deployments --github-config ~/bin/github_config.yaml --config ~/bin/jira_epic_config.yaml

opsgenie:
	python lucille/opsgenie_graph.py -c ~/bin/graphs.yaml -f ~/Desktop/debris/$(DATE_PREFIX)_opsgenie.csv
	python lucille/opsgenie_alerts_chart_weeks.py --csv ~/Desktop/debris/$(DATE_PREFIX)_opsgenie.csv --weeks 6 --output $(2X2_DIR)/opsgenie/$(DATE_PREFIX)_opsgenie_alerts_last_6_weeks.png

# Vendor spend (AWS, Databricks, Datadog). AWS Cost Explorer auth uses the
# standard boto3 credential chain (~/.aws/credentials by default).
vendor_spend:
	python -m lucille.vendor_spend.fetch_vendor_spend --config ~/bin/vendor_spend.yaml
	python -m lucille.vendor_spend.graph_vendor_spend --config ~/bin/vendor_spend.yaml --csv ~/Desktop/debris/$(DATE_PREFIX)_vendor_spend.csv

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
	@mkdir -p ~/Desktop/debris/archive && find $(2X2_DIR) -type f \( -name "*.csv" -o -name "*.txt" -o -name "*.png" \) -exec mv {} ~/Desktop/debris/archive/ \;

publish:
	python lucille/publish.py --output-dir $(2X2_DIR) --config ~/bin/jira_epic_config.yaml --layout ~/bin/confluence_engineering_page.json

support:
	python lucille/jira/sup_cycle_time.py --c ~/bin/jira.yaml --o $(2X2_DIR)/support
	python lucille/jira/sup_ticket_volume.py --c ~/bin/jira.yaml --o $(2X2_DIR)/support

2x2: deployments opsgenie github_security support lead_time publish

cost:
	python lucille/jira/epic_effort_summary.py --days 30 --output-dir ~/Desktop/debris --config ~/bin/jira.yaml

stale_jira:
	python lucille/jira/stale_tickets_to_csv.py ~/bin/jira_epic_config.yaml
	# edit CSV and make Jira comments with: python lucille/jira/comment_stale_tickets.py ${PATH_TO_CSV} ~/bin/jira_epic_config.yaml

lead_time:
	python lucille/lead_time_for_changes.py --config ~/bin/jira_epic_config.yaml
	python lucille/lead_time_report.py --input ~/Desktop/debris/$(DATE_PREFIX)_lead_time_changes_detailed.csv --output-dir ~/Desktop/debris/2x2/lead_time

mv_datadog:
	mv "$$(find ~/Downloads -name "*$$(date +%Y-%m-%d).csv" -maxdepth 1)" ~/Desktop/debris/$$(date +%Y_%m_%d)_datadog_spend.csv

datadog_spend:
	python -m lucille.vendor_spend.datadog_trends_csv --csv ~/Desktop/debris/$(DATE_PREFIX)_datadog_spend.csv


.PHONY: list

list:
	@echo "Available targets:"
	@$(MAKE) -pRrq -f $(firstword $(MAKEFILE_LIST)) : 2>/dev/null | \
		awk -v RS= -F: '/(^|\n)# Files(\n|$$)/,/(^|\n)# Finished Make data base/ { \
		if ($$1 !~ "^[#.]") { \
		print $$1 \
		} \
		}' | grep -E -v -e '^[^[:alnum:]]' -e '^$$@$$' | sort
