import json

from src.connectors.schemas import UserEvent


def serialize_event(event: UserEvent) -> bytes:
    return json.dumps(event.model_dump(mode="json")).encode("utf-8")


def deserialize_event(payload: bytes) -> UserEvent:
    data = json.loads(payload.decode("utf-8"))
    return UserEvent.model_validate(data)
