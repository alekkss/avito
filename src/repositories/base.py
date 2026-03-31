"""Абстрактный базовый репозиторий для работы с объявлениями аренды.

Определяет контракт (интерфейс), которому должна соответствовать
любая реализация хранилища данных. Сервисы зависят от этой
абстракции, а не от конкретной реализации (Dependency Inversion).
"""

from abc import ABC, abstractmethod

from src.models import RawListing


class BaseListingRepository(ABC):
    """Абстрактный репозиторий для хранения и чтения объявлений аренды.

    Определяет набор операций, необходимых бизнес-логике:
    сохранение объявлений, чтение, проверка существования
    и управление жизненным циклом хранилища.

    Все реализации (SQLite, PostgreSQL, in-memory) должны
    наследовать этот класс и реализовать все абстрактные методы.
    """

    @abstractmethod
    def initialize(self) -> None:
        """Инициализирует хранилище (создаёт таблицы, индексы и т.д.).

        Вызывается один раз при старте приложения.
        """

    @abstractmethod
    def save_listing(self, listing: RawListing) -> None:
        """Сохраняет одно объявление в хранилище.

        Если объявление с таким external_id уже существует —
        обновляет запись (upsert).

        Args:
            listing: Объявление аренды, полученное при парсинге.
        """

    @abstractmethod
    def save_listings(self, listings: list[RawListing]) -> None:
        """Сохраняет список объявлений в хранилище.

        Для каждого объявления: если запись с таким external_id
        существует — обновляет, иначе — создаёт новую.

        Args:
            listings: Список объявлений аренды.
        """

    @abstractmethod
    def get_all_listings(self) -> list[RawListing]:
        """Возвращает все объявления из хранилища.

        Returns:
            Список всех сохранённых объявлений аренды.
        """

    @abstractmethod
    def listing_exists(self, external_id: str) -> bool:
        """Проверяет, существует ли объявление с данным ID.

        Args:
            external_id: Уникальный идентификатор объявления
                в формате "av_<id>".

        Returns:
            True если объявление уже есть в хранилище.
        """

    @abstractmethod
    def get_listings_count(self) -> int:
        """Возвращает общее количество объявлений в хранилище.

        Returns:
            Количество записей.
        """

    @abstractmethod
    def close(self) -> None:
        """Закрывает соединение с хранилищем и освобождает ресурсы."""
