from src.connectors.generate_sample_events import generate_events
from src.connectors.io import deserialize_event, serialize_event


def test_event_round_trip_serialization() -> None:
    event = generate_events(num_users=1, events_per_user=1)[0]

    payload = serialize_event(event)
    restored = deserialize_event(payload)

    assert restored.event_id == event.event_id
    assert restored.entity_id == event.entity_id
    assert restored.event_type == event.event_type
    assert restored.schema_version == event.schema_version
