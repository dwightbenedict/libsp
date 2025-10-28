from sqlalchemy import String, Text, Integer, BigInteger, Float, Boolean, DateTime, ForeignKey, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Progress(Base):
    __tablename__ = "progress"

    institution_abbrv: Mapped[str] = mapped_column(
        ForeignKey("institution.abbrv"),
        primary_key=True
    )
    page_num: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    scraped: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    updated_at: Mapped[DateTime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False
    )


class Institution(Base):
    __tablename__ = "institution"

    id: Mapped[str] = mapped_column(Integer, primary_key=True)
    abbrv: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    doc_codes: Mapped[str] = mapped_column(Text, nullable=False)
    resource_types: Mapped[str] = mapped_column(Text, nullable=False)


class Record(Base):
    __tablename__ = "records"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    summary: Mapped[str] = mapped_column(Text, nullable=True)
    author: Mapped[str] = mapped_column(Text, nullable=True)
    publisher: Mapped[str] = mapped_column(Text, nullable=True)
    year_published: Mapped[str] = mapped_column(String, nullable=True)
    volume: Mapped[float] = mapped_column(Float, nullable=True)
    issue: Mapped[float] = mapped_column(Float, nullable=True)
    isbns: Mapped[str] = mapped_column(Text, nullable=True)
    language: Mapped[str] = mapped_column(String, nullable=True)
    country: Mapped[str] = mapped_column(String, nullable=True)
    has_ecopy: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    num_pages: Mapped[int] = mapped_column(Integer, nullable=True)
    doi: Mapped[str] = mapped_column(String, nullable=True)
    doc_type: Mapped[str] = mapped_column(String, nullable=False)
    subject: Mapped[str] = mapped_column(Text, nullable=True)
    tags: Mapped[str] = mapped_column(Text, nullable=True)


class Ebook(Base):
    __tablename__ = "ebooks"

    id: Mapped[int] = mapped_column(ForeignKey("records.id"), primary_key=True)
    read_url: Mapped[str] = mapped_column(Text, nullable=True)