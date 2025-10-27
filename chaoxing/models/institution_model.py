from pydantic import BaseModel
from chaoxing.utils import between


class Institution(BaseModel):
    """Represents institution metadata returned by the LibSP API."""

    id: int
    name: str
    hostname: str
    doc_codes: list[str]
    resource_types: list[str]

    @property
    def abbrv(self) -> str:
        """Return the institution abbreviation extracted from the hostname.

        Example:
            'findecnu.libsp.cn' → 'ecnu'
        """
        return between(self.hostname, "find", ".")


class InstitutionCreate(BaseModel):
    """Schema for creating an institution record in the database.

    Fields containing multiple values are stored as comma-separated strings.
    """

    id: int
    abbrv: str
    name: str
    doc_codes: str
    resource_types: str
