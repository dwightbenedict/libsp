import asyncio
import sys
from typing import Any
from pathlib import Path
import logging

import httpx
from tqdm import tqdm

from tusk.task_pool import TaskPool

from chaoxing.core.config import config

from chaoxing.api.institution import fetch_institution
from chaoxing.api.search import SearchParams, search_libsp

from chaoxing.models.institution_model import InstitutionCreate
from chaoxing.models.record_model import RecordCreate

from chaoxing.db.session import get_db_session

from chaoxing.services.institution_service import get_institution, create_institution
from chaoxing.services.record_service import create_records

from chaoxing.core.logging import setup_logging


LOG_FILE = Path("logs/chaoxing.log")
setup_logging(log_level=config.log_level, log_file=LOG_FILE)
logger = logging.getLogger("chaoxing")


def parse_record(item: dict[str, Any]) -> RecordCreate:
    isbns = item["isbns"]
    tags = item["chiSubjectClass"]
    return RecordCreate(
        id=item["recordId"],
        title=item["title"],
        summary=item["adstract"],
        author=item["author"],
        publisher=item["publisher"],
        year_published=item["publishYear"],
        volume=item["vol"],
        issue=item["issue"],
        isbns=", ".join(isbns) if isinstance(isbns, list) else isbns,
        language=item["langCode"],
        country=item["countryCode"],
        has_ecopy=bool(item["eCount"]),
        num_pages=item["pagesNum"],
        doi=item["doi"],
        doc_type=item["docName"],
        subject=item["subjectWord"],
        tags=", ".join(tags) if tags is not None else None,
    )


async def scrape_page(
    client: httpx.AsyncClient,
    base_params: SearchParams,
    page_num: int,
) -> None:
    try:
        params = base_params.copy(page_num=page_num)
        search_result = await search_libsp(client, params)
        records = [parse_record(item) for item in search_result.items]

        async with get_db_session() as db_conn:
            await create_records(db_conn, records)
    except Exception as e:
        logger.exception(f"Failed to scrape page {page_num} for {base_params.institution_abbrv}: {e}")


async def main() -> None:
    institution_hostname = "findecnu.libsp.cn"

    limits = httpx.Limits(
        max_connections=100,
        max_keepalive_connections=20,
        keepalive_expiry=30.0,
    )
    timeout = httpx.Timeout(15.0, read=30.0, write=15.0, pool=10.0)

    async with (
        httpx.AsyncClient(http2=True, limits=limits, timeout=timeout) as client,
        get_db_session() as db_session,
    ):
        institution = await fetch_institution(client, institution_hostname)

        institution_data = await get_institution(db_session, institution.id)
        if institution_data is None:
            institution_data = InstitutionCreate(
                id=institution.id,
                abbrv=institution.abbrv,
                name=institution.name,
                doc_codes=", ".join(institution.doc_codes),
                resource_types=", ".join(institution.resource_types),
            )
            await create_institution(db_session, institution_data)

        search_params = SearchParams(
            institution_abbrv=institution.abbrv,
            institution_id=institution.id,
            doc_codes=institution.doc_codes,
            resource_types=institution.resource_types,
            from_year=1700,
            to_year=2025,
            match_all=True
        )

        with tqdm(total=200, desc=f"Scraping {institution.abbrv}", file=sys.stderr) as pbar:
            async with TaskPool(config.concurrency_limit, progress_callback=pbar.update) as pool:
                for page_num in range(1, 201):
                    await pool.submit(scrape_page, client, search_params, page_num)
                await pool.join()

        logger.info(f"Scrape completed for {institution.abbrv}")


if __name__ == "__main__":
    asyncio.run(main())