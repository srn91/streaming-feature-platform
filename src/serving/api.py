from contextlib import asynccontextmanager

from fastapi import FastAPI, Request

from src.config import settings
from src.demo.bootstrap import bootstrap_hosted_demo
from src.features.offline_store import latest_feature_snapshot
from src.features.online_store import read_feature_snapshot
from src.quality.checks import build_quality_summary


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.demo_bootstrap = None
    if settings.hosted_demo:
        app.state.demo_bootstrap = bootstrap_hosted_demo(
            num_users=settings.demo_seed_users,
            events_per_user=settings.demo_events_per_user,
            sample_events_path=settings.demo_sample_events_path,
        )
    yield


app = FastAPI(title="streaming-feature-platform", lifespan=lifespan)


@app.get("/")
def root(request: Request) -> dict[str, object]:
    demo_bootstrap = getattr(request.app.state, "demo_bootstrap", None)
    return {
        "project": "streaming-feature-platform",
        "status": "running",
        "mode": "hosted_demo" if settings.hosted_demo else "local_full_stack",
        "demo_bootstrapped": demo_bootstrap is not None,
        "available_endpoints": ["/health", "/features/{entity_id}", "/quality/summary"],
        "example_entity_id": "user_0001",
    }


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "mode": "hosted_demo" if settings.hosted_demo else "local_full_stack"}


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
