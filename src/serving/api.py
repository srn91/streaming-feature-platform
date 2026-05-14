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
<style>
body{{margin:0;background:#f8fafc;color:#0f172a;font-family:-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;line-height:1.5}}
main{{max-width:1080px;margin:0 auto;padding:56px 24px}}.hero{{background:linear-gradient(135deg,#0f172a,#0f766e);color:white;border-radius:22px;padding:38px;box-shadow:0 24px 60px rgba(15,23,42,.18)}}
.eyebrow{{font-size:13px;letter-spacing:.12em;text-transform:uppercase;color:#99f6e4;font-weight:700}}h1{{font-size:42px;line-height:1.05;margin:10px 0 14px}}.hero p{{font-size:17px;color:#ccfbf1;max-width:760px}}
.grid{{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:14px;margin:22px 0}}.card{{background:white;border:1px solid #e2e8f0;border-radius:16px;padding:18px;box-shadow:0 10px 30px rgba(15,23,42,.06)}}
.metric{{font-size:25px;font-weight:800;color:#0f172a}}.label{{font-size:13px;color:#64748b;margin-top:3px}}.links{{display:flex;flex-wrap:wrap;gap:12px;margin-top:22px}}
a.button{{background:#0f172a;color:white;text-decoration:none;padding:11px 14px;border-radius:10px;font-weight:700}}a.secondary{{background:white;color:#0f172a;border:1px solid #cbd5e1}}
@media(max-width:800px){{.grid{{grid-template-columns:repeat(2,minmax(0,1fr))}}h1{{font-size:34px}}}}
</style></head>
<body><main>
<section class="hero"><div class="eyebrow">Feature infrastructure</div><h1>Streaming Feature Platform</h1>
<p>Hosted demo for event ingestion, feature materialization, online/offline quality checks, freshness monitoring, and training dataset export.</p>
<div class="links"><a class="button" href="/quality/summary">Quality summary</a><a class="button secondary" href="/features/user_0001">Sample feature lookup</a><a class="button secondary" href="/metrics">Metrics</a><a class="button secondary" href="/gcp/readiness">GCP readiness</a></div></section>
<section class="grid">
<div class="card"><div class="metric">running</div><div class="label">service status</div></div>
<div class="card"><div class="metric">{mode}</div><div class="label">execution mode</div></div>
<div class="card"><div class="metric">{bootstrap_status}</div><div class="label">demo data</div></div>
<div class="card"><div class="metric">user_0001</div><div class="label">sample entity</div></div>
</section>
<section class="card"><p>The root page gives a readable tour first. The JSON endpoints remain available for checking feature freshness, schema compatibility, training exports, and Prometheus-style service metrics.</p></section>
</main></body></html>"""


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
