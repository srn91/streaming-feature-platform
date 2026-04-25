from pathlib import Path

from confluent_kafka import Producer

from src.config import settings
from src.connectors.generate_sample_events import generate_events, write_jsonl
from src.connectors.io import serialize_event


def build_producer() -> Producer:
    return Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})


def publish_sample_events(num_users: int = 25, events_per_user: int = 20) -> int:
    producer = build_producer()
    events = generate_events(num_users=num_users, events_per_user=events_per_user)

    # Persist the generated batch so local debugging has an inspectable source of truth.
    write_jsonl(Path("data/generated/sample_events.jsonl"), events)

    for event in events:
        producer.produce(
            settings.raw_events_topic,
            key=event.entity_id.encode("utf-8"),
            value=serialize_event(event),
        )

    producer.flush()
    return len(events)


if __name__ == "__main__":
    count = publish_sample_events()
    print(f"Published {count} events to topic '{settings.raw_events_topic}'")
