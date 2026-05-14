from contextlib import asynccontextmanager
from time import perf_counter

from fastapi import FastAPI, Request, Response
from fastapi.responses import HTMLResponse

from src.config import settings
from src.demo.bootstrap import bootstrap_hosted_demo
from src.features.offline_store import latest_feature_snapshot
from src.gcp.assets import run_gcp_dry_run
from src.observability.metrics import observe_request, render_metrics, update_quality_metrics, update_training_metrics
from src.features.online_store import read_feature_snapshot
from src.quality.checks import build_quality_summary
from src.training.export_dataset import export_training_dataset_as_dict


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


@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    started_at = perf_counter()
    response = await call_next(request)
    observe_request(
        method=request.method,
        path=request.url.path,
        status_code=response.status_code,
        duration_seconds=perf_counter() - started_at,
    )
    return response


@app.get("/", response_class=HTMLResponse)
def root(request: Request) -> str:
    demo_bootstrap = getattr(request.app.state, "demo_bootstrap", None)
    mode = "hosted demo" if settings.hosted_demo else "local full stack"
    bootstrap_status = "loaded" if demo_bootstrap is not None else "not loaded"
    return f"""<!doctype html>
<html lang="en">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Streaming Feature Platform</title>
<style>body{{font-family:-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;max-width:860px;margin:48px auto;padding:0 24px;line-height:1.5;color:#111}}a{{color:#0645ad}}code{{background:#f3f4f6;padding:2px 5px;border-radius:4px}}</style></head>
<body>
<h1>Streaming Feature Platform</h1>
<p>Read-only hosted demo for a feature platform with event ingestion, feature materialization, quality checks, and monitoring output.</p>
<ul>
<li>Status: running</li>
<li>Mode: {mode}</li>
<li>Demo data: {bootstrap_status}</li>
</ul>
<h2>Open endpoints</h2>
<ul>
<li><a href="/health">Health check</a></li>
<li><a href="/features/user_0001">Sample feature lookup</a></li>
<li><a href="/quality/summary">Quality summary</a></li>
<li><a href="/training-dataset/summary">Training dataset summary</a></li>
<li><a href="/gcp/readiness">GCP readiness dry run</a></li>
<li><a href="/metrics">Prometheus metrics</a></li>
</ul>
</body></html>"""


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
    summary = build_quality_summary()
    update_quality_metrics(summary)
    return summary


@app.get("/training-dataset/summary")
def training_dataset_summary() -> dict[str, object]:
    summary = export_training_dataset_as_dict()
    update_training_metrics(summary)
    return summary


@app.get("/gcp/readiness")
def gcp_readiness() -> dict[str, object]:
    return run_gcp_dry_run()


@app.get("/metrics")
def metrics() -> Response:
    payload, content_type = render_metrics()
    return Response(content=payload, media_type=content_type)
