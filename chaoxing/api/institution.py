import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from chaoxing.models.institution_model import Institution


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPError)),
    reraise=True
)
async def fetch_institution(client: httpx.AsyncClient, hostname: str) -> Institution | None:
    url = f"https://{hostname}/find/groupResource/dict"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:144.0) Gecko/20100101 Firefox/144.0",
        "Accept": "application/json, text/plain, */*",
        "Origin": f"https://{hostname}",
        "Referer": f"https://{hostname}/"
    }
    response = await client.post(url, headers=headers)
    response.raise_for_status()

    data = response.json()

    if not data["success"]:
        return None

    result = data["data"]
    institution_data = result["libCode"][0]
    return Institution(
        id=int(institution_data["groupCode"]),
        name=institution_data["name"],
        hostname=hostname,
        doc_codes=[doc_code["code"] for doc_code in result["docCode"]],
        resource_types=[resource_type["code"] for resource_type in result["resourceType"]]
    )

