from typing import Any

from pydantic import BaseModel


class SearchResult(BaseModel):
    """Schema representing a LibSP search response."""

    total_records: int
    items: list[dict[str, Any]]


