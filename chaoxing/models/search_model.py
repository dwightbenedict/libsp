from typing import Any

from pydantic import BaseModel


class SearchResult(BaseModel):
    """Schema representing a LibSP search response."""

    count: int
    items: list[dict[str, Any]] | None = None
    stats: dict[str, dict[str, int]] | None = None


