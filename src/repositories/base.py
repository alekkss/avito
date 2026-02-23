"""Абстрактный базовый репозиторий для работы с товарами.

Определяет контракт (интерфейс), которому должна соответствовать
любая реализация хранилища данных. Сервисы зависят от этой
абстракции, а не от конкретной реализации (Dependency Inversion).
"""

from abc import ABC, abstractmethod

from src.models import NormalizedProduct, RawProduct


class BaseProductRepository(ABC):
    """Абстрактный репозиторий для хранения и чтения товаров.

    Определяет набор операций, необходимых бизнес-логике:
    сохранение сырых и нормализованных товаров, чтение,
    проверка существования и управление жизненным циклом хранилища.
    """

    @abstractmethod
    def initialize(self) -> None:
        """Инициализирует хранилище (создаёт таблицы, индексы и т.д.).

        Вызывается один раз при старте приложения.
        """

    @abstractmethod
    def save_raw_product(self, product: RawProduct) -> None:
        """Сохраняет один сырой товар в хранилище.

        Если товар с таким avito_id уже существует, обновляет запись.

        Args:
            product: Сырой товар, полученный при парсинге.
        """

    @abstractmethod
    def save_raw_products(self, products: list[RawProduct]) -> None:
        """Сохраняет список сырых товаров в хранилище.

        Для каждого товара: если запись с таким avito_id существует —
        обновляет, иначе — создаёт новую.

        Args:
            products: Список сырых товаров.
        """

    @abstractmethod
    def get_all_raw_products(self) -> list[RawProduct]:
        """Возвращает все сырые товары из хранилища.

        Returns:
            Список всех сохранённых сырых товаров.
        """

    @abstractmethod
    def get_raw_products_without_normalization(self) -> list[RawProduct]:
        """Возвращает сырые товары, ещё не прошедшие AI-нормализацию.

        Returns:
            Список товаров, для которых нет записи в нормализованных.
        """

    @abstractmethod
    def save_normalized_product(self, product: NormalizedProduct) -> None:
        """Сохраняет один нормализованный товар.

        Args:
            product: Товар после AI-нормализации.
        """

    @abstractmethod
    def save_normalized_products(
        self, products: list[NormalizedProduct]
    ) -> None:
        """Сохраняет список нормализованных товаров.

        Args:
            products: Список товаров после AI-нормализации.
        """

    @abstractmethod
    def get_all_normalized_products(self) -> list[NormalizedProduct]:
        """Возвращает все нормализованные товары.

        Returns:
            Список всех нормализованных товаров для экспорта.
        """

    @abstractmethod
    def raw_product_exists(self, avito_id: str) -> bool:
        """Проверяет, существует ли сырой товар с данным ID.

        Args:
            avito_id: Уникальный идентификатор объявления Avito.

        Returns:
            True если товар уже есть в хранилище.
        """

    @abstractmethod
    def get_raw_products_count(self) -> int:
        """Возвращает общее количество сырых товаров в хранилище.

        Returns:
            Количество записей.
        """

    @abstractmethod
    def get_normalized_products_count(self) -> int:
        """Возвращает количество нормализованных товаров в хранилище.

        Returns:
            Количество записей.
        """

    @abstractmethod
    def close(self) -> None:
        """Закрывает соединение с хранилищем и освобождает ресурсы."""
