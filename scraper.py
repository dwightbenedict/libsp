import sys
import math
import logging
from pathlib import Path
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
    result = await search_libsp(client, params.copy(count_only=True))
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
    client: httpx.AsyncClient, params: SearchParams, db_factory: async_sessionmaker[AsyncSession]
) -> None:
    try:
        result = await search_libsp(client, params)
        if not result.items:
            return
        records = [parse_record(item) for item in result.items]
        async with get_db_session(db_factory) as db:
            await create_records(db, records)
        logger.info(f"Added {len(records)} records to DB.")
    except Exception as e:
        logger.exception(f"Failed to scrape {params.page=} for {params.institution_abbrv}: {e}")


def compute_total_pages(records_count: int, max_rows: int, max_pages: int) -> int:
    return min(max_pages, math.ceil(records_count / max_rows))


async def scrape_institution(institution_hostname: str, db_url: str) -> None:
    db_factory = create_session_factory(db_url)
    limits = httpx.Limits(max_connections=100, max_keepalive_connections=20, keepalive_expiry=30.0)
    timeout = httpx.Timeout(15.0, read=30.0, write=15.0, pool=10.0)

    async with (
        httpx.AsyncClient(http2=True, limits=limits, timeout=timeout) as client,
        get_db_session(db_factory) as db,
    ):
        institution = await fetch_institution(client, institution_hostname)

        existing = await get_institution(db, institution.id)
        if existing is None:
            await create_institution(
                db,
                InstitutionCreate(
                    id=institution.id,
                    abbrv=institution.abbrv,
                    name=institution.name,
                    doc_codes=", ".join(institution.doc_codes),
                    resource_types=", ".join(institution.resource_types),
                ),
            )

        filters = await fetch_search_filters(client, institution.id, institution.abbrv)

        sort_fields = ["relevance", "issued_sort", "class_no_sort_s"]
        sort_clauses = ["asc", "desc"]
        max_rows = 50
        max_pages = 200
        max_records = 10_000
        start_year, end_year = 1850, 2025

        with tqdm(desc=f"Scraping {institution.abbrv}", file=sys.stderr) as pbar:
            async with TaskPool(config.concurrency_limit, progress_callback=pbar.update) as pool:
                for filter_key, filter_values in filters.items():
                    for filter_value in filter_values:
                        base = SearchParams(
                            institution_abbrv=institution.abbrv,
                            institution_id=institution.id,
                            rows=max_rows,
                            match_all=True,
                        )
                        setattr(base, filter_key, [filter_value])

                        total = await fetch_records_count(client, base)
                        if total == 0:
                            continue

                        # base case — small enough, scrape directly
                        if total <= max_records:
                            total_pages = compute_total_pages(total, max_rows, max_pages)
                            for page in range(1, total_pages + 1):
                                await pool.submit(scrape_page, client, base.copy(page=page), db_factory)
                            continue

                        # refine by year
                        for year in range(start_year, end_year + 1):
                            year_params = base.copy(from_year=year, to_year=year)
                            count_year = await fetch_records_count(client, year_params)
                            if count_year == 0:
                                continue

                            # if small enough now → scrape directly
                            if count_year <= max_records:
                                pages = compute_total_pages(count_year, max_rows, max_pages)
                                for page in range(1, pages + 1):
                                    await pool.submit(scrape_page, client, year_params.copy(page=page), db_factory)
                                continue

                            # refine by sort_field
                            for sort_field in sort_fields:
                                field_params = year_params.copy(sort_field=sort_field)
                                count_field = await fetch_records_count(client, field_params)
                                if count_field == 0:
                                    continue

                                # if small enough after sort_field → scrape
                                if count_field <= max_records:
                                    pages = compute_total_pages(count_field, max_rows, max_pages)
                                    for page in range(1, pages + 1):
                                        await pool.submit(
                                            scrape_page, client, field_params.copy(page=page), db_factory
                                        )
                                    continue

                                # still too large → refine by sort_clause
                                for sort_clause in sort_clauses:
                                    clause_params = field_params.copy(sort_clause=sort_clause)
                                    count_clause = await fetch_records_count(client, clause_params)
                                    if count_clause == 0:
                                        continue
                                    pages = compute_total_pages(count_clause, max_rows, max_pages)
                                    for page in range(1, pages + 1):
                                        await pool.submit(
                                            scrape_page, client, clause_params.copy(page=page), db_factory
                                        )
                await pool.join()

        logger.info(f"Scrape completed for {institution.abbrv}")
        engine = db_factory.kw["bind"]
        await engine.dispose()
