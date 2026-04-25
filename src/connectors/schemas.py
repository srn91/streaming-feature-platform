from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class EventType(str, Enum):
    PAGE_VIEW = "page_view"
    CLICK = "click"
    ADD_TO_CART = "add_to_cart"
    PURCHASE = "purchase"


class EventMetadata(BaseModel):
    session_id: str
    page: str
    product_id: str | None = None
    category: str | None = None
    price: float | None = None
    attributes: dict[str, Any] = Field(default_factory=dict)


class UserEvent(BaseModel):
    event_id: str
    entity_id: str
    event_type: EventType
    event_ts: datetime
    schema_version: str = "1.0.0"
    metadata: EventMetadata

