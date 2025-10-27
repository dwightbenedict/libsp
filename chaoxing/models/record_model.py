from pydantic import BaseModel


class RecordCreate(BaseModel):
    """Schema for creating a bibliographic record in the database."""

    id: int
    title: str
    summary: str | None = None
    author: str | None = None
    publisher: str | None = None
    year_published: str | None = None
    volume: float | None = None
    issue: float | None = None
    isbns: str | None = None
    language: str | None = None
    country: str | None = None
    has_ecopy: bool = False
    num_pages: int | None = None
    doi: str | None = None
    doc_type: str
    subject: str | None = None
    tags: str | None = None
