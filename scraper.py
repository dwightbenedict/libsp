import sys
import math
import logging
from pathlib import Path
from itertools import product
from typing import Any

import httpx
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from tqdm import tqdm

from tusk.task_pool import TaskPool
from chaoxing.core.config import config
from chaoxing.api.institution import fetch_institution
from chaoxing.api.search import SearchParams, search_libsp
from chaoxing.models.institution_model import InstitutionCreate
from chaoxing.models.record_model import RecordCreate
from chaoxing.db.session import create_session_factory, get_db_session
from chaoxing.services.institution_service import get_institution, create_institution
from chaoxing.services.record_service import create_records
from chaoxing.core.logging import setup_logging


LOG_FILE = Path("logs/chaoxing.log")
setup_logging(log_level=config.log_level, log_file=LOG_FILE)

logging.getLogger("httpx").setLevel(logging.ERROR)

logger = logging.getLogger("chaoxing")


async def fetch_search_filters(
    client: httpx.AsyncClient,
    institution_id: int,
    institution_abbrv: str
) -> dict[str, list[str]]:
    params = SearchParams(
        institution_abbrv=institution_abbrv,
        institution_id=institution_id,
        count_only=True,
    )
    result = await search_libsp(client, params)
    stats = result.stats

    return {
        "doc_codes": list(stats["docCode"].keys()),
        "resource_types": list(stats["resourceType"].keys()),
        "lit_codes": list(stats["litCode"].keys()),
        "subjects": list(stats["subject"].keys()),
        "authors": list(stats["author"].keys()),
        "publishers": list(stats["publisher"].keys()),
        "discodes": list(stats["discode1"].keys()),
        "lib_codes": list(stats["libCode"].keys()),
        "ecollection_ids": list(stats["neweCollectionIds"].keys()),
        "core_includes": list(stats["coreIncludes"].keys()),
        "location_ids": list(stats["locationId"].keys()),
        "current_location_ids": list(stats["curLocationId"].keys()),
        "campus_ids": list(stats["campusId"].keys()),
        "kind_no": list(stats["kindNo"].keys()),
        "groups": list(stats["group"].keys()),
        "lang_codes": list(stats["langCode"].keys()),
        "country_codes": list(stats["countryCode"].keys()),
    }


async def fetch_records_count(client: httpx.AsyncClient, params: SearchParams) -> int:
    count_params = params.copy(count_only=True)
    result = await search_libsp(client, count_params)
    return result.count


def parse_record(item: dict[str, Any]) -> RecordCreate:
    isbns = item.get("isbns")
    tags = item.get("chiSubjectClass")
    return RecordCreate(
        id=item["recordId"],
        title=item["title"],
        summary=item.get("adstract"),
        author=item.get("author"),
        publisher=item.get("publisher"),
        year_published=item.get("publishYear"),
        volume=item.get("vol"),
        issue=item.get("issue"),
        isbns=", ".join(isbns) if isinstance(isbns, list) else isbns,
        language=item.get("langCode"),
        country=item.get("countryCode"),
        has_ecopy=bool(item.get("eCount")),
        num_pages=item.get("pagesNum"),
        doi=item.get("doi"),
        doc_type=item.get("docName"),
        subject=item.get("subjectWord"),
        tags=", ".join(tags) if tags else None,
    )


async def scrape_page(
        client: httpx.AsyncClient, params: SearchParams, db_session_factory: async_sessionmaker[AsyncSession]
) -> None:
    try:
        result = await search_libsp(client, params)
        items = result.items

        if not items:
            logger.info("No records found.")
            return

        records = [parse_record(item) for item in result.items]
        async with get_db_session(db_session_factory) as db_conn:
            await create_records(db_conn, records)
        logger.info(f"Added {len(records)} records to the database.")
    except Exception as e:
        logger.exception(f"Failed to scrape {params.page=} for {params.institution_abbrv}: {e}")


async def scrape_institution(institution_hostname: str, db_url: str) -> None:
    db_session_factory = create_session_factory(db_url)

    limits = httpx.Limits(max_connections=100, max_keepalive_connections=20, keepalive_expiry=30.0)
    timeout = httpx.Timeout(15.0, read=30.0, write=15.0, pool=10.0)

    async with (
        httpx.AsyncClient(http2=True, limits=limits, timeout=timeout) as client,
        get_db_session(db_session_factory) as db_session,
    ):
        institution = await fetch_institution(client, institution_hostname)

        existing = await get_institution(db_session, institution.id)
        if existing is None:
            data = InstitutionCreate(
                id=institution.id,
                abbrv=institution.abbrv,
                name=institution.name,
                doc_codes=", ".join(institution.doc_codes),
                resource_types=", ".join(institution.resource_types),
            )
            await create_institution(db_session, data)

        filters = await fetch_search_filters(client, institution.id, institution.abbrv)

        sort_fields = ["relevance", "issued_sort", "class_no_sort_s"]
        sort_clauses = ["asc", "desc"]
        sorting_pairs = list(product(sort_fields, sort_clauses))

        max_rows = 50
        max_pages = 200
        max_records = 10_000
        start_year = 1850
        end_year = 2025

        with tqdm(desc=f"Scraping {institution.abbrv}", file=sys.stderr) as pbar:
            async with TaskPool(config.concurrency_limit, progress_callback=pbar.update) as pool:
                for filter_key, filter_values in filters.items():
                    for filter_value in filter_values:
                        base_params = SearchParams(
                            institution_abbrv=institution.abbrv,
                            institution_id=institution.id,
                            rows=max_rows,
                            match_all=True,
                        )
                        setattr(base_params, filter_key, [filter_value])

                        total_records = await fetch_records_count(client, base_params)
                        total_pages = max(1, min(max_pages, math.ceil(total_records / max_rows)))

                        # if more than 10k records, refine using sorting
                        if total_records > max_records:
                            for pub_year in range(start_year, end_year + 1):
                                for sort_field, sort_clause in sorting_pairs:
                                    subtotal_params = base_params.copy(
                                        from_year=pub_year,
                                        to_year=pub_year,
                                        sort_field=sort_field,
                                        sort_clause=sort_clause,
                                    )
                                    subtotal_records = await fetch_records_count(client, subtotal_params)
                                    subtotal_pages = max(
                                        1, min(max_pages, math.ceil(subtotal_records / max_rows))
                                    )
                                    for page in range(1, subtotal_pages + 1):
                                        page_params = subtotal_params.copy(page=page)
                                        logger.info(
                                            f"Scraping {institution.abbrv} | "
                                            f"{filter_key}={filter_value} | "
                                            f"year={pub_year} | "
                                            f"{sort_field}={sort_clause} | "
                                            f"page={page}/{total_pages} | "
                                            f"subtotal={subtotal_records:,} | "
                                            f"total={total_records:,}"
                                        )
                                        await pool.submit(scrape_page, client, page_params, db_session_factory)
                        else:
                            for page in range(1, total_pages + 1):
                                page_params = base_params.copy(page=page)
                                logger.info(
                                    f"Scraping {institution.abbrv} | "
                                    f"{filter_key}={filter_value} | "
                                    f"page={page}/{total_pages} | "
                                    f"total={total_records}"
                                )
                                await pool.submit(scrape_page, client, page_params, db_session_factory)

                await pool.join()

        logger.info(f"Scrape completed for {institution.abbrv}")
        engine = db_session_factory.kw["bind"]
        await engine.dispose()

