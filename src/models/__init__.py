"""Пакет доменных моделей.

Предоставляет модели данных для объявлений краткосрочной аренды:
    from src.models import RawListing, RoomCategory
"""

from src.models.product import RawListing, RoomCategory

__all__ = [
    "RawListing",
    "RoomCategory",
]
