from datetime import datetime

from pydantic import BaseModel


class FeatureSnapshot(BaseModel):
    entity_id: str
    as_of_ts: datetime
    feature_version: str = "v1"
    clicks_1h: int
    sessions_24h: int
    purchase_value_24h: float
    last_active_ts: datetime
    click_to_impression_ratio_24h: float
    dominant_category: str | None = None
    dominant_category_share: float = 0.0

