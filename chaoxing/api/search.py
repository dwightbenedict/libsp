from dataclasses import dataclass, field, replace

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from chaoxing.models.search_model import SearchResult


@dataclass
class SearchParams:
    institution_abbrv: str
    institution_id: int
    doc_codes: list[str]
    query: str = "*"
    resource_types: list[str] = field(default_factory=lambda: ["1", "2", "3"])
    page_num: int = 1
    page_size: int = 50
    count_only: bool = False
    match_all: bool = False

    def copy(self, **overrides):
        return replace(self, **overrides)


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
        "docCode": params.doc_codes,
        "searchFieldContent": params.query if not params.match_all else "*",
        "resourceType": params.resource_types,
        "page": params.page_num,
        "rows": params.page_size if not params.count_only else 0,
    }
    response = await client.post(url, headers=headers, json=payload)
    response.raise_for_status()
    data = response.json()
    return SearchResult(
        total_records=data["data"]["numFound"],
        items=data["data"]["searchResult"]
    )