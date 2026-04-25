import json

from src.config import settings
from src.features.definitions import FeatureSnapshot


def _client():
    import redis

    return redis.Redis.from_url(settings.redis_url, decode_responses=True)


def feature_key(entity_id: str) -> str:
    return f"{settings.online_feature_prefix}{entity_id}"


def write_feature_snapshot(snapshot: FeatureSnapshot) -> None:
    client = _client()
    client.set(feature_key(snapshot.entity_id), json.dumps(snapshot.model_dump(mode="json")))


def read_feature_snapshot(entity_id: str) -> FeatureSnapshot | None:
    client = _client()
    payload = client.get(feature_key(entity_id))
    if payload is None:
        return None
    return FeatureSnapshot.model_validate_json(payload)
