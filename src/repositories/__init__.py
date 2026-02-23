"""Пакет репозиториев для хранения данных.

Предоставляет абстракцию и реализацию хранилища товаров:
    from src.repositories import BaseProductRepository, SQLiteProductRepository
"""

from src.repositories.base import BaseProductRepository
from src.repositories.sqlite_repository import SQLiteProductRepository

__all__ = [
    "BaseProductRepository",
    "SQLiteProductRepository",
]
