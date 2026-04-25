.PHONY: setup up down test produce consume materialize run-all clean-data reset-local

setup:
	python3 -m pip install -r requirements.txt

up:
	docker compose up -d

down:
	docker compose down

test:
	pytest tests

produce:
	python3 -m src.connectors.producer

consume:
	python3 -m src.pipelines.raw_event_consumer

materialize:
	python3 -m src.features.materialize_features

run-all: produce consume materialize

clean-data:
	rm -f data/generated/raw_events.duckdb data/generated/features.duckdb data/generated/sample_events.jsonl

reset-local: clean-data down
