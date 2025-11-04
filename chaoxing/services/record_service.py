from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert

from chaoxing.models.record_model import RecordCreate
from chaoxing.db.schema import Record


async def create_records(session: AsyncSession, records: list[RecordCreate]) -> int:
    """Insert multiple records in bulk for improved performance."""

    if not records:
        return 0

    values = [record.model_dump() for record in records]
    stmt = (
        insert(Record)
        .values(values)
        .on_conflict_do_nothing(index_elements=["id"])
        .returning(Record.id)
    )

    result = await session.execute(stmt)
    inserted = result.scalars().all()
    await session.commit()
    return len(inserted)