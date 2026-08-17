# Local entry points. Every target uses the project venv so the system
# Python is never mixed in. Override the interpreter with:
#   make setup PYTHON=python3.11
PYTHON ?= python3.11
VENV := .venv
BIN := $(VENV)/bin
export PYTHONPATH := $(CURDIR)/src
# PySpark needs a real JDK. Homebrew is not required: setup installs Temurin 17 here.
JAVA_HOME ?= $(HOME)/.jdks/temurin-17
export JAVA_HOME
export PATH := $(JAVA_HOME)/bin:$(PATH)

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
	@if [ ! -x "$(JAVA_HOME)/bin/java" ]; then \
		echo "Installing Temurin JDK 17 to $(JAVA_HOME) (PySpark runtime dependency, not a pip package)."; \
		curl -L --fail --retry 3 -o /tmp/temurin17.tar.gz \
		  "https://api.adoptium.net/v3/binary/latest/17/ga/mac/x64/jdk/hotspot/normal/eclipse?project=jdk"; \
		rm -rf /tmp/temurin17-extract && mkdir -p /tmp/temurin17-extract; \
		tar -xzf /tmp/temurin17.tar.gz -C /tmp/temurin17-extract; \
		HOME_DIR=$$(find /tmp/temurin17-extract -type d -name Home | head -1); \
		mkdir -p "$$(dirname $(JAVA_HOME))"; \
		rm -rf "$(JAVA_HOME)"; \
		cp -R "$$HOME_DIR" "$(JAVA_HOME)"; \
	fi
	@$(JAVA_HOME)/bin/java -version

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
