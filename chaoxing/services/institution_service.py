from sqlalchemy.ext.asyncio import AsyncSession

from chaoxing.models.institution_model import InstitutionCreate
from chaoxing.db.schema import Institution


async def get_institution(session: AsyncSession, institution_id: int) -> Institution | None:
    return await session.get(Institution, institution_id)


async def create_institution(session: AsyncSession, data: InstitutionCreate) -> Institution:
    ebook = Institution(**data.model_dump())
    session.add(ebook)
    await session.commit()
    await session.refresh(ebook)
    return ebook
