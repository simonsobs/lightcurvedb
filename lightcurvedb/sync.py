"""
Synchronous database engine and session management.
"""

from sqlmodel import Session, SQLModel, create_engine

from .config import settings

engine = create_engine(settings.postgres_uri, echo=settings.postgres_echo)


def setup_tables():
    SQLModel.metadata.create_all(engine)


def get_session():
    return Session(engine)
