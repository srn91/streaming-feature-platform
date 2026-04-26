PYTHON ?= python3.12
HOST ?= 0.0.0.0
PORT ?= 8010

.PHONY: setup up down test produce consume materialize export-training serve run-all clean-data reset-local demo-bootstrap

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

export-training:
	$(PYTHON) -m src.training.export_dataset

serve:
	$(PYTHON) -m uvicorn src.serving.api:app --host $(HOST) --port $(PORT)

demo-bootstrap:
	HOSTED_DEMO=1 $(PYTHON) -m src.demo.bootstrap

run-all: produce consume materialize

clean-data:
	rm -f data/generated/raw_events.duckdb data/generated/features.duckdb data/generated/sample_events.jsonl

reset-local: clean-data down
