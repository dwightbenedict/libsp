from dataclasses import dataclass, field, replace

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from chaoxing.models.search_model import SearchResult


@dataclass
class SearchParams:
    institution_abbrv: str
    institution_id: int
    query: str = "*"
    page: int = 1
    rows: int = 50
    from_year: int = 1850
    to_year: int = 2025
    sort_field: str = "relevance"
    sort_clause: str = "desc"
    doc_codes: list[str] = field(default_factory=list)
    resource_types: list[str] = field(default_factory=list)
    lit_codes: list[str] = field(default_factory=list)
    subjects: list[str] = field(default_factory=list)
    authors: list[str] = field(default_factory=list)
    publishers: list[str] = field(default_factory=list)
    discodes: list[str] = field(default_factory=list)
    lib_codes: list[str] = field(default_factory=list)
    ecollection_ids: list[str] = field(default_factory=list)
    core_include: list[str] = field(default_factory=list)
    location_ids: list[str] = field(default_factory=list)
    current_location_ids: list[str] = field(default_factory=list)
    campus_ids: list[str] = field(default_factory=list)
    kind_no: list[str] = field(default_factory=list)
    groups: list[str] = field(default_factory=list)
    lang_codes: list[str] = field(default_factory=list)
    country_codes: list[str] = field(default_factory=list)
    count_only: bool = False
    match_all: bool = True

    def copy(self, **overrides):
        return replace(self, **overrides)


class SearchError(Exception):
    pass


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPError)),
    reraise=True
)
async def search_libsp(client: httpx.AsyncClient, params: SearchParams) -> SearchResult:
    base_url = f"https://find{params.institution_abbrv}.libsp.cn"
    url = f"{base_url}/find/unify/search"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:144.0) Gecko/20100101 Firefox/144.0",
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json;charset=utf-8",
        "Groupcode": str(params.institution_id),
        "Origin": base_url,
        "Referer": f"{base_url}/"
    }
    payload = {
        "searchFieldContent": params.query if not params.match_all else "*",
        "page": params.page,
        "rows": params.rows if not params.count_only else 0,
        "sortField": params.sort_field,
        "sortClause": params.sort_clause,
        "docCode": params.doc_codes,
        "resourceType": params.resource_types,
        "litCode": params.lit_codes,
        "author": params.authors,
        "publisher": params.publishers,
        "subject": params.subjects,
        "discode1": params.discodes,
        "libCode": params.lib_codes,
        "locationId": params.location_ids,
        "curLocationId": params.current_location_ids,
        "campusId": params.campus_ids,
        "neweCollectionIds": params.ecollection_ids,
        "kindNo": params.kind_no,
        "langCode": params.lang_codes,
        "countryCode": params.country_codes,
        "group": params.groups,
        "newCoreInclude": params.core_include,
    }
    response = await client.post(url, headers=headers, json=payload)
    response.raise_for_status()
    data = response.json()

    if not data["success"]:
        raise SearchError(f"An error occured: {data['message']}")

    return SearchResult(
        count=data["data"]["numFound"],
        items=data["data"]["searchResult"],
        stats=data["data"]["facetResult"]
    )