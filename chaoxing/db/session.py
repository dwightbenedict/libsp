from typing import AsyncGenerator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine


def create_session_factory(db_url: str) -> async_sessionmaker[AsyncSession]:
    engine: AsyncEngine = create_async_engine(
        db_url,
        echo=False,
        pool_size=5,
        max_overflow=5,
    )
    return async_sessionmaker(engine, expire_on_commit=False)


@asynccontextmanager
async def get_db_session(session_factory: async_sessionmaker[AsyncSession]) -> AsyncGenerator[AsyncSession, None]:
    async with session_factory() as session:
        try:
            yield session
        finally:
            await session.close()
