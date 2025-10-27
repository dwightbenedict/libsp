import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPError)),
    reraise=True
)
async def fetch_ebook_url(client: httpx.AsyncClient, hostname: str, record_id: int) -> str | None:
    url = f"https://{hostname}/find/ePortfolio/itemList"
    params = {
        "recordId": record_id,
        "page": 1,
        "rows": 1
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:144.0) Gecko/20100101 Firefox/144.0",
        "Accept": "application/json, text/plain, */*",
        "Origin": f"https://{hostname}",
        "Referer": f"https://{hostname}/"
    }
    response = await client.get(url, params=params, headers=headers)
    response.raise_for_status()

    data = response.json()

    if not data["success"]:
        return None

    sources = data["data"]["list"]
    return sources[0]["url"] if sources else None

