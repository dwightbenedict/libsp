from typing import AsyncGenerator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from chaoxing.core.config import config


def create_session_factory(db_url: str) -> async_sessionmaker[AsyncSession]:
    engine: AsyncEngine = create_async_engine(
        db_url,
        pool_size=config.db_pool_size,
        max_overflow=config.db_max_overflow,
        echo=config.debug,
    )
    return async_sessionmaker(engine, expire_on_commit=False)


@asynccontextmanager
async def get_db_session(session_factory: async_sessionmaker[AsyncSession]) -> AsyncGenerator[AsyncSession, None]:
    async with session_factory() as session:
        try:
            yield session
        finally:
            await session.close()
