# Deployment settings
HOST        ?= root@your-server
REMOTE_BIN  ?= /usr/local/bin/telemt-monthly
REMOTE_CONF ?= /etc/telemt
STATE_DIR   ?= /var/lib/telemt-monthly
OUT_DIR     ?= /var/log/telemt-monthly
CRON_SCHEDULE ?= */30 * * * *

# Google Sheets (leave empty to disable)
GSHEET_SA_KEY        ?=
GSHEET_SPREADSHEET_ID ?=

.PHONY: deploy setup install cron upload-key status logs uninstall

## Full deploy: setup dirs, install script, configure cron
deploy: setup install cron
	@echo "Done. Run 'make status HOST=$(HOST)' to verify."

## Create directories on remote server
setup:
	ssh $(HOST) 'mkdir -p $(STATE_DIR) $(OUT_DIR) $(REMOTE_CONF)'

## Copy script to remote server
install:
	scp telemt_monthly.py $(HOST):$(REMOTE_BIN)
	ssh $(HOST) 'chmod +x $(REMOTE_BIN)'
	@echo "Installed $(REMOTE_BIN)"

## Set up cron job
cron:
ifdef GSHEET_SPREADSHEET_ID
	ssh $(HOST) 'cat > /etc/cron.d/telemt-monthly << EOF\n\
GSHEET_ENABLED=1\n\
GSHEET_SA_KEY=$(REMOTE_CONF)/sa-key.json\n\
GSHEET_SPREADSHEET_ID=$(GSHEET_SPREADSHEET_ID)\n\
$(CRON_SCHEDULE) root $(REMOTE_BIN) >> $(OUT_DIR)/cron.log 2>&1\n\
EOF'
else
	ssh $(HOST) 'cat > /etc/cron.d/telemt-monthly << EOF\n\
$(CRON_SCHEDULE) root $(REMOTE_BIN) >> $(OUT_DIR)/cron.log 2>&1\n\
EOF'
endif
	@echo "Cron installed: $(CRON_SCHEDULE)"

## Upload Google service account key
upload-key:
ifndef GSHEET_SA_KEY
	$(error Set GSHEET_SA_KEY=/path/to/local/sa-key.json)
endif
	scp $(GSHEET_SA_KEY) $(HOST):$(REMOTE_CONF)/sa-key.json
	ssh $(HOST) 'chmod 600 $(REMOTE_CONF)/sa-key.json'
	@echo "Key uploaded to $(REMOTE_CONF)/sa-key.json"

## Show service status on remote
status:
	@echo "=== Last cron runs ==="
	ssh $(HOST) 'tail -20 $(OUT_DIR)/cron.log 2>/dev/null || echo "No logs yet"'
	@echo ""
	@echo "=== Current month totals ==="
	ssh $(HOST) 'cat $(OUT_DIR)/$$(date +%Y-%m)-totals.csv 2>/dev/null || echo "No totals yet"'

## Tail remote logs
logs:
	ssh $(HOST) 'tail -f $(OUT_DIR)/cron.log'

## Run once on remote (dry-run)
dry-run:
	ssh $(HOST) '$(REMOTE_BIN) --dry-run'

## Run once on remote
run:
	ssh $(HOST) '$(REMOTE_BIN)'

## Remove everything from remote
uninstall:
	ssh $(HOST) 'rm -f $(REMOTE_BIN) /etc/cron.d/telemt-monthly'
	@echo "Removed. State in $(STATE_DIR) and logs in $(OUT_DIR) preserved."

## Show help
help:
	@echo "Usage: make <target> HOST=root@your-server"
	@echo ""
	@echo "Targets:"
	@echo "  deploy      Full deploy (setup + install + cron)"
	@echo "  install     Copy script to server"
	@echo "  upload-key  Upload Google SA key (GSHEET_SA_KEY=/path/to/key.json)"
	@echo "  status      Show last runs and current totals"
	@echo "  logs        Tail remote cron log"
	@echo "  dry-run     Run once in dry-run mode"
	@echo "  run         Run once for real"
	@echo "  uninstall   Remove script and cron (keeps data)"
	@echo ""
	@echo "Examples:"
	@echo "  make deploy HOST=root@10.0.0.1"
	@echo "  make deploy HOST=root@10.0.0.1 GSHEET_SPREADSHEET_ID=abc123"
	@echo "  make upload-key HOST=root@10.0.0.1 GSHEET_SA_KEY=~/sa-key.json"
	@echo "  make status HOST=root@10.0.0.1"
