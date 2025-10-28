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

    gs = func.generate_series(1, max_page).table_valued("page").alias("gs")

    stmt = (
        select(gs.c.page)
        .select_from(
            gs.outerjoin(
                Progress,
                (Progress.institution_abbrv == institution_abbrv)
                & (Progress.page_num == gs.c.page)
                & (Progress.scraped.is_(True)),
            )
        )
        .where(Progress.page_num.is_(None))
        .order_by(gs.c.page)
        .limit(1)
    )
    result = await session.execute(stmt)
    first_gap = result.scalar_one_or_none()
    return first_gap or (max_page + 1)
