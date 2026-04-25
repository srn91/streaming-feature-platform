import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from random import Random
from uuid import uuid4

from src.connectors.schemas import EventMetadata, EventType, UserEvent


rand = Random(42)

PRODUCTS = [
    ("prod_1001", "electronics"),
    ("prod_1002", "fashion"),
    ("prod_1003", "home"),
    ("prod_1004", "books"),
    ("prod_1005", "sports"),
]

PAGES = ["/home", "/search", "/product", "/cart", "/checkout"]


def build_event(entity_id: str, base_time: datetime) -> UserEvent:
    product_id, category = rand.choice(PRODUCTS)
    event_type = rand.choices(
        population=[EventType.PAGE_VIEW, EventType.CLICK, EventType.ADD_TO_CART, EventType.PURCHASE],
        weights=[55, 25, 12, 8],
        k=1,
    )[0]
    price = round(rand.uniform(9.99, 299.99), 2) if event_type == EventType.PURCHASE else None

    metadata = EventMetadata(
        session_id=f"session_{str(uuid4())[:8]}",
        page=rand.choice(PAGES),
        product_id=product_id,
        category=category,
        price=price,
        attributes={
            "device": rand.choice(["mobile", "desktop", "tablet"]),
            "region": rand.choice(["us_east", "us_west", "eu", "apac"]),
        },
    )

    return UserEvent(
        event_id=str(uuid4()),
        entity_id=entity_id,
        event_type=event_type,
        event_ts=base_time,
        metadata=metadata,
    )


def generate_events(num_users: int = 25, events_per_user: int = 20) -> list[UserEvent]:
    start = datetime.now(UTC) - timedelta(hours=24)
    events: list[UserEvent] = []

    for user_idx in range(num_users):
        entity_id = f"user_{user_idx + 1:04d}"
        current_time = start + timedelta(minutes=rand.randint(0, 120))
        for _ in range(events_per_user):
            current_time += timedelta(minutes=rand.randint(1, 90))
            events.append(build_event(entity_id, current_time))

    return sorted(events, key=lambda event: event.event_ts)


def write_jsonl(path: Path, events: list[UserEvent]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for event in events:
            handle.write(json.dumps(event.model_dump(mode="json")) + "\n")


if __name__ == "__main__":
    output_path = Path("data/generated/sample_events.jsonl")
    events = generate_events()
    write_jsonl(output_path, events)
    print(f"Wrote {len(events)} events to {output_path}")
