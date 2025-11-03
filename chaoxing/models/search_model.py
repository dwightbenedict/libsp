from typing import Any

from pydantic import BaseModel, Field


class SearchStats(BaseModel):
    doc_codes: dict[str, int] | None = Field(default_factory=dict, alias="docCode")
    resource_types: dict[str, int] | None = Field(default_factory=dict, alias="resourceType")
    lit_codes: dict[str, int] | None = Field(default_factory=dict, alias="litCode")
    subjects: dict[str, int] | None = Field(default_factory=dict, alias="subject")
    authors: dict[str, int] | None = Field(default_factory=dict, alias="author")
    publishers: dict[str, int] | None = Field(default_factory=dict, alias="publisher")
    discodes: dict[str, int] | None = Field(default_factory=dict, alias="discode1")
    lib_codes: dict[str, int] | None = Field(default_factory=dict, alias="libCode")
    ecollection_ids: dict[str, int] | None = Field(default_factory=dict, alias="neweCollectionIds")
    core_includes: dict[str, int] | None = Field(default_factory=dict, alias="coreIncludes")
    location_ids: dict[str, int] | None = Field(default_factory=dict, alias="locationId")
    current_location_ids: dict[str, int] | None = Field(default_factory=dict, alias="curLocationId")
    campus_ids: dict[str, int] | None = Field(default_factory=dict, alias="campusId")
    kind_no: dict[str, int] | None = Field(default_factory=dict, alias="kindNo")
    groups: dict[str, int] | None = Field(default_factory=dict, alias="group")
    lang_codes: dict[str, int] | None = Field(default_factory=dict, alias="langCode")
    country_codes: dict[str, int] | None = Field(default_factory=dict, alias="countryCode")

    def to_filter_dict(self) -> dict[str, list[str]]:
        return {
            key: list(value.keys())
            for key, value in self.model_dump().items()
            if value
        }


class SearchResult(BaseModel):
    """Schema representing a LibSP search response."""

    count: int
    items: list[dict[str, Any]] | None = None
    stats: dict[str, dict[str, int]] | None = None


