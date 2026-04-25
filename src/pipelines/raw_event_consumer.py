from confluent_kafka import Consumer

from src.config import settings
from src.connectors.io import deserialize_event
from src.pipelines.raw_event_store import persist_raw_event


def build_consumer() -> Consumer:
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": "raw-event-persister",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([settings.raw_events_topic])
    return consumer


def consume_and_persist(max_messages: int | None = None) -> int:
    consumer = build_consumer()
    consumed = 0

    try:
        empty_polls = 0
        while True:
            message = consumer.poll(1.0)
            if message is None:
                empty_polls += 1
                if empty_polls >= 2:
                    break
                continue
            if message.error():
                raise RuntimeError(str(message.error()))

            event = deserialize_event(message.value())
            persist_raw_event(event)
            consumed += 1
            if max_messages is not None and consumed >= max_messages:
                break
    finally:
        consumer.close()

    return consumed


if __name__ == "__main__":
    count = consume_and_persist()
    print(f"Consumed and persisted {count} events from topic '{settings.raw_events_topic}'")
