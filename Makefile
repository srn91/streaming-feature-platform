PYTHON ?= python3.12

.PHONY: setup up down test produce consume materialize run-all clean-data reset-local

setup:
	$(PYTHON) -m pip install -r requirements.txt

up:
	docker compose up -d

down:
	docker compose down

test:
	$(PYTHON) -m pytest tests

produce:
	$(PYTHON) -m src.connectors.producer

consume:
	$(PYTHON) -m src.pipelines.raw_event_consumer

materialize:
	$(PYTHON) -m src.features.materialize_features

run-all: produce consume materialize

clean-data:
	rm -f data/generated/raw_events.duckdb data/generated/features.duckdb data/generated/sample_events.jsonl

reset-local: clean-data down
