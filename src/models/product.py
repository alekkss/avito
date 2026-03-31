"""Доменные модели объявлений аренды.

Содержит dataclass-модель для представления объявления
краткосрочной аренды на Avito:
    - RawListing: данные, извлечённые из карточки объявления
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class RoomCategory(Enum):
    """Категория жилья по количеству комнат.

    Используется для классификации объявлений аренды.
    Значения соответствуют стандартной классификации Avito.
    """

    ROOM = "Комната"
    STUDIO = "Студия"
    ONE = "1к"
    TWO = "2к"
    THREE = "3к"
    FOUR_PLUS = "4к+"
    UNKNOWN = "Неизвестно"


@dataclass
class RawListing:
    """Объявление краткосрочной аренды, извлечённое с Avito.

    Содержит 13 обязательных полей из ТЗ и резервное поле
    analytics_payload для будущей аналитики. Поля событий
    (price_change_event, booking_block_event, cancellation_event)
    на Этапе 1 остаются пустыми — заполняются Delta Engine
    на Этапе 2.

    Attributes:
        external_id: Уникальный идентификатор объявления
            в формате "av_<id>" для склейки данных между источниками.
        latitude: Широта объекта (координаты).
        longitude: Долгота объекта (координаты).
        room_category: Категория жилья (Комната, Студия, 1к–4к+).
        price_60_days: Массив цен на 60 дней вперёд.
            Индекс 0 — сегодня, индекс 59 — через 59 дней.
            Значение 0 означает, что цена не указана.
        calendar_60_days: Массив занятости на 60 дней вперёд.
            0 — свободен, 1 — занят.
        snapshot_timestamp: Время фиксации данных (UTC).
        last_host_update: Время последнего обновления календаря хостом.
            None если информация недоступна.
        min_stay: Минимальный срок аренды в сутках.
        is_instant_book: Наличие мгновенного бронирования.
        host_rating: Рейтинг хоста (0.0 если не указан).
        price_change_event: Данные события изменения цены.
            Заполняется Delta Engine (Этап 2). На Этапе 1 — None.
        booking_block_event: Данные события бронирования (блок дат).
            Заполняется Delta Engine (Этап 2). На Этапе 1 — None.
        cancellation_event: Данные события отмены бронирования.
            Заполняется Delta Engine (Этап 2). На Этапе 1 — None.
        analytics_payload: Резервное поле для будущей аналитики.
            Хранится как JSONB в БД. На Этапе 1 — None.
        url: Относительная ссылка на объявление на Avito.
        title: Оригинальное название объявления (для справки).
    """

    external_id: str
    latitude: float
    longitude: float
    room_category: RoomCategory
    price_60_days: list[int]
    calendar_60_days: list[int]
    snapshot_timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    last_host_update: datetime | None = None
    min_stay: int = 1
    is_instant_book: bool = False
    host_rating: float = 0.0
    price_change_event: dict[str, Any] | None = None
    booking_block_event: dict[str, Any] | None = None
    cancellation_event: dict[str, Any] | None = None
    analytics_payload: dict[str, Any] | None = None
    url: str = ""
    title: str = ""

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

    @property
    def coordinates(self) -> tuple[float, float]:
        """Координаты объекта в виде кортежа (lat, lon).

        Returns:
            Кортеж (широта, долгота).
        """
        return (self.latitude, self.longitude)

    @property
    def occupancy_rate(self) -> float:
        """Процент занятости на ближайшие 60 дней.

        Returns:
            Доля занятых дней от 0.0 до 1.0.
        """
        if not self.calendar_60_days:
            return 0.0
        occupied = sum(self.calendar_60_days)
        return occupied / len(self.calendar_60_days)

    @property
    def average_price(self) -> float:
        """Средняя цена за сутки по свободным дням.

        Считает среднюю цену только по тем дням, где цена > 0
        и день свободен (calendar = 0).

        Returns:
            Средняя цена или 0.0 если нет данных.
        """
        if not self.price_60_days or not self.calendar_60_days:
            return 0.0

        prices = [
            price
            for price, occupied in zip(
                self.price_60_days, self.calendar_60_days
            )
            if price > 0 and occupied == 0
        ]

        if not prices:
            return 0.0
        return sum(prices) / len(prices)
