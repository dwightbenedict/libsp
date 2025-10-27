from pydantic import BaseModel


class EbookCreate(BaseModel):
    """Schema for creating an eBook record in the database.

    Represents a digital copy associated with a bibliographic record.
    The `id` corresponds to the same identifier used by the Record entry.
    """

    id: int
    read_url: str | None = None
