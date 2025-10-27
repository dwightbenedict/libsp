from sqlalchemy.ext.asyncio import AsyncSession

from chaoxing.models.ebook_model import EbookCreate
from chaoxing.db.schema import Ebook


async def create_ebook(session: AsyncSession, data: EbookCreate) -> Ebook:
    ebook = Ebook(**data.model_dump())
    session.add(ebook)
    await session.commit()
    await session.refresh(ebook)
    return ebook