from fastapi import FastAPI

from src.features.offline_store import latest_feature_snapshot
from src.features.online_store import read_feature_snapshot
from src.quality.checks import build_quality_summary


app = FastAPI(title="streaming-feature-platform")


@app.get("/")
def root() -> dict[str, object]:
    return {
        "project": "streaming-feature-platform",
        "status": "running",
        "available_endpoints": ["/health", "/features/{entity_id}", "/quality/summary"],
        "example_entity_id": "user_0001",
    }


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/features/{entity_id}")
def get_features(entity_id: str) -> dict[str, object]:
    try:
        online_snapshot = read_feature_snapshot(entity_id)
        if online_snapshot is not None:
            return {"source": "redis", "features": online_snapshot.model_dump(mode="json")}
    except Exception:
        pass

    offline_snapshot = latest_feature_snapshot(entity_id)
    if offline_snapshot is not None:
        return {"source": "offline_store", "features": offline_snapshot.model_dump(mode="json")}

    return {"entity_id": entity_id, "message": "No features found for entity"}


@app.get("/quality/summary")
def quality_summary() -> dict[str, object]:
    return build_quality_summary()
