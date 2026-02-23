"""Пакет доменных моделей.

Предоставляет модели данных для всех этапов обработки товаров:
    from src.models import RawProduct, NormalizedProduct, raw_to_normalized
"""

from src.models.product import NormalizedProduct, RawProduct, raw_to_normalized

__all__ = [
    "NormalizedProduct",
    "RawProduct",
    "raw_to_normalized",
]
