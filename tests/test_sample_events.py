from src.connectors.generate_sample_events import generate_events


def test_generate_events_returns_sorted_events() -> None:
    events = generate_events(num_users=2, events_per_user=3)

    assert len(events) == 6
    assert events == sorted(events, key=lambda event: event.event_ts)
    assert all(event.entity_id.startswith("user_") for event in events)
    assert all(event.schema_version == "1.0.0" for event in events)
