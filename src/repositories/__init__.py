"""Пакет репозиториев для хранения данных.

Предоставляет абстракцию и реализацию хранилища объявлений аренды:
    from src.repositories import BaseListingRepository, SQLiteListingRepository
"""

from src.repositories.base import BaseListingRepository
from src.repositories.sqlite_repository import SQLiteListingRepository

__all__ = [
    "BaseListingRepository",
    "SQLiteListingRepository",
]
