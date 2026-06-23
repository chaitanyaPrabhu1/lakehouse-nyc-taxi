.DEFAULT_GOAL := help
SHELL := /bin/bash
VENV := .venv
PY := $(VENV)/bin/python
PIP := $(VENV)/bin/pip
TF := terraform -chdir=terraform

.PHONY: help venv test local-run quality dbt-build dbt-docs plan apply destroy clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

venv: ## Create .venv and install dev dependencies
	python3 -m venv $(VENV)
	$(PIP) install --quiet --upgrade pip
	$(PIP) install --quiet -r requirements-dev.txt
	@echo "venv ready -> activate with: source $(VENV)/bin/activate"

test: ## Run unit tests (moto-mocked S3, synthetic parquet)
	$(PY) -m pytest -q

local-run: ## Download a sample month + zones into ./.local_lake (real data, local FS)
	STORAGE_BACKEND=local LOCAL_LAKE_DIR=.local_lake $(PY) scripts/local_run.py

quality: ## Run the bronze data-quality gate over the local lake (pre-dbt)
	STORAGE_BACKEND=local LOCAL_LAKE_DIR=.local_lake $(PY) quality/validate_bronze.py

dbt-build: ## dbt deps + seed + run + test (requires DATA_BUCKET etc. exported)
	cd dbt && dbt deps && dbt seed --target prod && dbt run --target prod && dbt test --target prod

dbt-docs: ## Generate the dbt docs site + lineage graph
	cd dbt && dbt docs generate --target prod

plan: ## terraform plan (requires AWS creds)
	$(TF) init -input=false
	$(TF) plan

apply: ## terraform apply
	$(TF) init -input=false
	$(TF) apply

destroy: ## terraform destroy (tear everything down)
	$(TF) destroy

clean: ## Remove build artifacts and the local lake
	rm -rf build .local_lake dbt/target dbt/dbt_packages dbt/logs .pytest_cache
