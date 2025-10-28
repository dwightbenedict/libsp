from sqlalchemy import exists, select, func, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from chaoxing.db.schema import Progress


async def is_page_scraped(session: AsyncSession, institution_abbrv: str, page_num: int) -> bool:
    stmt = select(
        exists().where(
            Progress.institution_abbrv == institution_abbrv,
            Progress.page_num == page_num,
            Progress.scraped.is_(True),
        )
    )
    result = await session.execute(stmt)
    return result.scalar()


async def mark_page_scraped(session: AsyncSession, institution_abbrv: str, page_num: int) -> None:
    stmt = (
        insert(Progress)
        .values(institution_abbrv=institution_abbrv, page_num=page_num, scraped=True)
        .on_conflict_do_update(
            index_elements=["institution_abbrv", "page_num"],
            set_={"scraped": True}
        )
    )
    await session.execute(stmt)
    await session.commit()


async def get_scraped_count(session: AsyncSession, institution_abbrv: str) -> int:
    stmt = (
        select(func.count())
        .where(
            Progress.institution_abbrv == institution_abbrv,
            Progress.scraped.is_(True)
        )
    )
    result = await session.execute(stmt)
    return result.scalar_one()


async def _get_max_scraped_page(session: AsyncSession, institution_abbrv: str) -> int:
    result = await session.execute(
        select(func.coalesce(func.max(Progress.page_num), 0))
        .where(Progress.institution_abbrv == institution_abbrv)
    )
    return result.scalar_one()


async def get_start_page(session: AsyncSession, institution_abbrv: str) -> int:
    max_page = await _get_max_scraped_page(session, institution_abbrv)

    if max_page == 0:
        return 1  # no records yet → start from page 1

    stmt = text("""
        SELECT gs.page
        FROM generate_series(:start::INTEGER, :end::INTEGER) AS gs(page)
        LEFT OUTER JOIN progress 
            ON progress.institution_abbrv = :abbrv::VARCHAR
            AND progress.page_num = gs.page
            AND progress.scraped IS true
        WHERE progress.page_num IS NULL
        ORDER BY gs.page
        LIMIT :limit
    """)
    result = await session.execute(stmt)
    first_gap = result.scalar_one_or_none()
    return first_gap or (max_page + 1)
