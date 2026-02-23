"""Доменные модели товаров.

Содержит dataclass-модели для представления товаров на разных
этапах обработки:
    - RawProduct: сырые данные, извлечённые со страницы Avito
    - NormalizedProduct: данные после AI-нормализации с группировкой
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass(frozen=True)
class RawProduct:
    """Сырой товар, извлечённый из каталога Avito.

    Содержит данные в том виде, в котором они получены со страницы,
    без какой-либо обработки или нормализации.

    Attributes:
        avito_id: Уникальный идентификатор объявления на Avito.
        title: Оригинальное название товара из объявления.
        price: Цена в рублях (целое число).
        url: Относительная ссылка на страницу объявления.
        description: Текст описания из сниппета (может быть обрезан).
        image_url: Ссылка на основное изображение товара.
        seller_name: Имя или название продавца.
        seller_rating: Рейтинг продавца (например, "4,7").
        seller_reviews: Текст с количеством отзывов (например, "128 отзывов").
        scraped_at: Дата и время парсинга (UTC).
    """

    avito_id: str
    title: str
    price: int
    url: str
    description: str = ""
    image_url: str = ""
    seller_name: str = ""
    seller_rating: str = ""
    seller_reviews: str = ""
    scraped_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    @property
    def full_url(self) -> str:
        """Полная ссылка на объявление с доменом Avito.

        Returns:
            Абсолютный URL объявления.
        """
        base = "https://www.avito.ru"
        if self.url.startswith("http"):
            return self.url
        return f"{base}{self.url}"


@dataclass(frozen=True)
class NormalizedProduct:
    """Товар после AI-нормализации.

    Содержит как оригинальные данные, так и результат обработки
    моделью AI — нормализованное название для группировки
    одинаковых товаров.

    Attributes:
        avito_id: Уникальный идентификатор объявления на Avito.
        original_title: Оригинальное название из объявления.
        normalized_title: Нормализованное AI название для группировки.
        product_category: Категория товара, определённая AI.
        key_specs: Ключевые характеристики, извлечённые AI.
        price: Цена в рублях.
        url: Относительная ссылка на объявление.
        full_url: Абсолютный URL объявления.
        description: Текст описания из сниппета.
        image_url: Ссылка на основное изображение.
        seller_name: Имя или название продавца.
        seller_rating: Рейтинг продавца.
        seller_reviews: Количество отзывов продавца.
        scraped_at: Дата и время парсинга (UTC).
        normalized_at: Дата и время нормализации AI (UTC).
    """

    avito_id: str
    original_title: str
    normalized_title: str
    product_category: str
    key_specs: str
    price: int
    url: str
    full_url: str
    description: str = ""
    image_url: str = ""
    seller_name: str = ""
    seller_rating: str = ""
    seller_reviews: str = ""
    scraped_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    normalized_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )


def raw_to_normalized(
    raw: RawProduct,
    normalized_title: str,
    product_category: str,
    key_specs: str,
) -> NormalizedProduct:
    """Создаёт NormalizedProduct из RawProduct и результатов AI.

    Фабричная функция, которая объединяет сырые данные с результатами
    AI-нормализации в единую сущность. Сохраняет все оригинальные
    поля из RawProduct.

    Args:
        raw: Исходный сырой товар с Avito.
        normalized_title: Нормализованное название от AI.
        product_category: Категория товара от AI.
        key_specs: Ключевые характеристики от AI.

    Returns:
        Новый экземпляр NormalizedProduct.
    """
    return NormalizedProduct(
        avito_id=raw.avito_id,
        original_title=raw.title,
        normalized_title=normalized_title,
        product_category=product_category,
        key_specs=key_specs,
        price=raw.price,
        url=raw.url,
        full_url=raw.full_url,
        description=raw.description,
        image_url=raw.image_url,
        seller_name=raw.seller_name,
        seller_rating=raw.seller_rating,
        seller_reviews=raw.seller_reviews,
        scraped_at=raw.scraped_at,
        normalized_at=datetime.now(timezone.utc),
    )
