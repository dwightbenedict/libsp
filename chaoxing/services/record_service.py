from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert

from chaoxing.models.record_model import RecordCreate
from chaoxing.db.schema import Record


async def create_records(session: AsyncSession, records: list[RecordCreate]) -> None:
    """Insert multiple records in bulk for improved performance."""

    if not records:
        return

    values = [record.model_dump() for record in records]
    stmt = insert(Record).values(values)
    stmt = stmt.on_conflict_do_nothing(index_elements=["id"])
    await session.execute(stmt)
    await session.commit()