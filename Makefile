# Local entry points. Every target uses the project venv so the system
# Python is never mixed in. Override the interpreter with:
#   make setup PYTHON=python3.11
PYTHON ?= python3.11
VENV := .venv
BIN := $(VENV)/bin
export PYTHONPATH := $(CURDIR)/src

.PHONY: setup generate run test recon clean help

help:
	@echo "make setup     create venv and install pinned dependencies"
	@echo "make generate  write synthetic SAS-DW extracts and frozen baselines"
	@echo "make run       bronze -> silver -> gold -> all 9 reports"
	@echo "make recon     compare new reports against frozen SAS baselines"
	@echo "make test      run unit and smoke tests"
	@echo "make clean     delete generated data (keeps the venv)"

setup:
	@command -v $(PYTHON) >/dev/null 2>&1 || { echo "ERROR: $(PYTHON) not found. Install Python 3.11."; exit 1; }
	$(PYTHON) -m venv $(VENV)
	$(BIN)/pip install --upgrade pip
	$(BIN)/pip install -r requirements.txt
	@java -version >/dev/null 2>&1 || echo "WARNING: no working JDK. PySpark needs JDK 11 or 17 before make generate / make run. On this Mac, /usr/bin/java can exist without a runtime."

generate:
	$(BIN)/python -m xpankki_esg.pipeline generate --as-of 2025-12-31

run:
	$(BIN)/python -m xpankki_esg.pipeline run --layer all --as-of 2025-12-31

test:
	$(BIN)/pytest tests/ -v

recon:
	$(BIN)/python -m xpankki_esg.pipeline recon --as-of 2025-12-31

clean:
	rm -rf data/landing data/lakehouse data/baselines data/output
