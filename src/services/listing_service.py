"""Сервис парсинга карточки объявления краткосрочной аренды.

Заходит на страницу конкретного объявления Avito и извлекает
детальные данные: координаты, календарь занятости, цены на
60 дней вперёд, условия аренды и информацию о хосте.

Паттерн Strategy: при переходе на другую площадку (Суточно.ру)
можно создать альтернативную реализацию с тем же интерфейсом.
"""

import asyncio
import json
import random
from datetime import datetime, timezone

from playwright.async_api import Page

from src.config import get_logger
from src.models import RawListing, RoomCategory
from src.services.browser_service import BrowserService

logger = get_logger("listing_service")

# Таймаут ожидания элементов на странице карточки (мс)
CARD_ELEMENT_TIMEOUT: int = 10000

# Задержка между переходами на карточки (секунды)
CARD_NAVIGATE_DELAY_MIN: float = 3.0
CARD_NAVIGATE_DELAY_MAX: float = 7.0

# Маппинг текстовых описаний категорий на enum
ROOM_CATEGORY_MAP: dict[str, RoomCategory] = {
    "комната": RoomCategory.ROOM,
    "студия": RoomCategory.STUDIO,
    "1-к": RoomCategory.ONE,
    "1к": RoomCategory.ONE,
    "1-комн": RoomCategory.ONE,
    "однокомнатная": RoomCategory.ONE,
    "2-к": RoomCategory.TWO,
    "2к": RoomCategory.TWO,
    "2-комн": RoomCategory.TWO,
    "двухкомнатная": RoomCategory.TWO,
    "3-к": RoomCategory.THREE,
    "3к": RoomCategory.THREE,
    "3-комн": RoomCategory.THREE,
    "трёхкомнатная": RoomCategory.THREE,
    "трехкомнатная": RoomCategory.THREE,
    "4-к": RoomCategory.FOUR_PLUS,
    "4к": RoomCategory.FOUR_PLUS,
    "4-комн": RoomCategory.FOUR_PLUS,
    "четырёхкомнатная": RoomCategory.FOUR_PLUS,
    "четырехкомнатная": RoomCategory.FOUR_PLUS,
    "5-к": RoomCategory.FOUR_PLUS,
    "5к": RoomCategory.FOUR_PLUS,
    "5-комн": RoomCategory.FOUR_PLUS,
    "многокомнатная": RoomCategory.FOUR_PLUS,
}

# JavaScript для извлечения координат из карты Avito.
# Avito использует Яндекс.Карты; координаты хранятся
# в data-атрибутах контейнера карты или в глобальном
# объекте конфигурации страницы.
JS_EXTRACT_COORDINATES: str = """
() => {
    // Способ 1: data-атрибуты контейнера карты
    const mapEl = document.querySelector(
        '[data-marker="item-view/item-map"]'
    );
    if (mapEl) {
        const lat = mapEl.getAttribute('data-map-lat');
        const lon = mapEl.getAttribute('data-map-lon');
        if (lat && lon) {
            return { latitude: parseFloat(lat), longitude: parseFloat(lon) };
        }
    }

    // Способ 2: ищем координаты в JSON-LD разметке
    const scripts = document.querySelectorAll(
        'script[type="application/ld+json"]'
    );
    for (const script of scripts) {
        try {
            const data = JSON.parse(script.textContent);
            if (data.geo) {
                return {
                    latitude: parseFloat(data.geo.latitude),
                    longitude: parseFloat(data.geo.longitude)
                };
            }
        } catch (e) {}
    }

    // Способ 3: ищем в __initialData__ или window.dataLayer
    try {
        const pageData = window.__initialData__;
        if (pageData) {
            const text = JSON.stringify(pageData);
            const coordMatch = text.match(
                /"coords":\\s*\\{\\s*"lat":\\s*([\\d.]+),\\s*"lng":\\s*([\\d.]+)/
            );
            if (coordMatch) {
                return {
                    latitude: parseFloat(coordMatch[1]),
                    longitude: parseFloat(coordMatch[2])
                };
            }
        }
    } catch (e) {}

    // Способ 4: ищем в атрибутах любого элемента с координатами
    const allElements = document.querySelectorAll('[data-lat][data-lon]');
    if (allElements.length > 0) {
        const el = allElements[0];
        return {
            latitude: parseFloat(el.getAttribute('data-lat')),
            longitude: parseFloat(el.getAttribute('data-lon'))
        };
    }

    return { latitude: 0.0, longitude: 0.0 };
}
"""

# JavaScript для извлечения данных календаря и цен.
# На странице посуточной аренды Avito данные календаря
# загружаются через внутренний API и могут быть найдены
# в глобальных переменных или в DOM-элементах.
JS_EXTRACT_CALENDAR: str = """
() => {
    const result = {
        prices: [],
        calendar: [],
        minStay: 1,
        isInstantBook: false
    };

    // Способ 1: ищем данные в __initialData__
    try {
        const pageData = window.__initialData__;
        if (pageData) {
            const text = JSON.stringify(pageData);

            // Ищем массив цен
            const pricesMatch = text.match(/"prices":\\s*(\\[[^\\]]*\\])/);
            if (pricesMatch) {
                try {
                    result.prices = JSON.parse(pricesMatch[1]);
                } catch (e) {}
            }

            // Ищем календарь занятости
            const calendarMatch = text.match(
                /"calendar":\\s*(\\[[^\\]]*\\])/
            );
            if (calendarMatch) {
                try {
                    result.calendar = JSON.parse(calendarMatch[1]);
                } catch (e) {}
            }

            // Минимальный срок
            const minStayMatch = text.match(/"minDays":\\s*(\\d+)/);
            if (minStayMatch) {
                result.minStay = parseInt(minStayMatch[1]);
            }

            // Мгновенное бронирование
            if (text.includes('"instantBooking":true')
                || text.includes('"isInstantBooking":true')) {
                result.isInstantBook = true;
            }
        }
    } catch (e) {}

    // Способ 2: ищем элементы календаря в DOM
    if (result.calendar.length === 0) {
        const calendarDays = document.querySelectorAll(
            '[data-marker*="calendar"] [data-day]'
        );
        calendarDays.forEach(day => {
            const isBooked = day.classList.contains('booked')
                || day.classList.contains('disabled')
                || day.getAttribute('data-booked') === 'true';
            result.calendar.push(isBooked ? 1 : 0);
        });
    }

    // Способ 3: ищем мгновенное бронирование в DOM
    if (!result.isInstantBook) {
        const instantEl = document.querySelector(
            '[data-marker*="instant-booking"]'
        );
        if (instantEl) {
            result.isInstantBook = true;
        }
        const pageText = document.body.innerText;
        if (pageText.includes('Мгновенное бронирование')
            || pageText.includes('Забронировать сейчас')) {
            result.isInstantBook = true;
        }
    }

    return result;
}
"""


class ListingService:
    """Сервис для парсинга карточки объявления краткосрочной аренды.

    Переходит на страницу конкретного объявления и извлекает
    все детальные данные, недоступные из каталога: координаты,
    календарь, цены на 60 дней, условия аренды.

    Attributes:
        _browser_service: Сервис управления браузером.
    """

    def __init__(self, browser_service: BrowserService) -> None:
        """Инициализирует сервис парсинга карточек.

        Args:
            browser_service: Сервис для управления браузером.
        """
        self._browser_service = browser_service

    async def parse_listing(
        self,
        page: Page,
        external_id: str,
        url: str,
        title: str,
        base_price: int,
    ) -> RawListing | None:
        """Парсит одну карточку объявления аренды.

        Переходит на страницу объявления, извлекает все детальные
        данные и возвращает заполненный объект RawListing.

        Args:
            page: Активная страница Playwright.
            external_id: Идентификатор объявления (формат "av_<id>").
            url: Относительная или абсолютная ссылка на объявление.
            title: Название объявления из каталога.
            base_price: Базовая цена из каталога (руб./сут.).

        Returns:
            Заполненный RawListing или None при ошибке парсинга.
        """
        full_url = url if url.startswith("http") else f"https://www.avito.ru{url}"

        delay = random.uniform(CARD_NAVIGATE_DELAY_MIN, CARD_NAVIGATE_DELAY_MAX)
        logger.debug(
            "listing_navigation_delay",
            external_id=external_id,
            delay=round(delay, 1),
        )
        await asyncio.sleep(delay)

        try:
            await page.goto(
                full_url,
                wait_until="domcontentloaded",
                timeout=60000,
            )
            await asyncio.sleep(random.uniform(3.0, 5.0))
        except Exception as e:
            logger.warning(
                "listing_navigation_failed",
                external_id=external_id,
                url=full_url[:200],
                error=str(e),
            )
            return None

        # Проверяем блокировку
        is_blocked = await self._browser_service._check_blocked()
        if is_blocked:
            logger.warning(
                "listing_page_blocked",
                external_id=external_id,
            )
            return None

        # Извлекаем данные
        coordinates = await self._extract_coordinates(page, external_id)
        calendar_data = await self._extract_calendar_data(page, external_id)
        room_category = await self._extract_room_category(page, title)
        host_rating = await self._extract_host_rating(page)
        last_host_update = await self._extract_last_update(page)

        # Формируем массив цен на 60 дней
        prices_60 = calendar_data.get("prices", [])
        if not prices_60:
            # Если цены не найдены — заполняем базовой ценой из каталога
            prices_60 = [base_price] * 60

        # Дополняем до 60 элементов при необходимости
        prices_60 = self._pad_array(prices_60, 60, base_price)
        calendar_60 = self._pad_array(
            calendar_data.get("calendar", []), 60, 0
        )

        listing = RawListing(
            external_id=external_id,
            latitude=coordinates["latitude"],
            longitude=coordinates["longitude"],
            room_category=room_category,
            price_60_days=prices_60,
            calendar_60_days=calendar_60,
            snapshot_timestamp=datetime.now(timezone.utc),
            last_host_update=last_host_update,
            min_stay=calendar_data.get("minStay", 1),
            is_instant_book=calendar_data.get("isInstantBook", False),
            host_rating=host_rating,
            url=url,
            title=title,
        )

        logger.info(
            "listing_parsed",
            external_id=external_id,
            room_category=room_category.value,
            latitude=coordinates["latitude"],
            longitude=coordinates["longitude"],
            occupancy=f"{listing.occupancy_rate:.0%}",
            avg_price=round(listing.average_price),
        )

        return listing

    async def _extract_coordinates(
        self, page: Page, external_id: str
    ) -> dict[str, float]:
        """Извлекает координаты объекта со страницы.

        Использует JavaScript-инъекцию для поиска координат
        в различных источниках: data-атрибуты карты, JSON-LD,
        глобальные переменные.

        Args:
            page: Активная страница Playwright.
            external_id: ID объявления для логирования.

        Returns:
            Словарь с ключами latitude и longitude.
        """
        try:
            coords = await page.evaluate(JS_EXTRACT_COORDINATES)
            if coords and coords.get("latitude", 0) != 0:
                logger.debug(
                    "coordinates_extracted",
                    external_id=external_id,
                    latitude=coords["latitude"],
                    longitude=coords["longitude"],
                )
                return coords
        except Exception as e:
            logger.warning(
                "coordinates_js_extraction_failed",
                external_id=external_id,
                error=str(e),
            )

        # Фоллбэк: ищем координаты в тексте страницы
        try:
            content = await page.content()
            coords = self._find_coordinates_in_html(content)
            if coords["latitude"] != 0.0:
                logger.debug(
                    "coordinates_extracted_from_html",
                    external_id=external_id,
                    latitude=coords["latitude"],
                    longitude=coords["longitude"],
                )
                return coords
        except Exception as e:
            logger.warning(
                "coordinates_html_extraction_failed",
                external_id=external_id,
                error=str(e),
            )

        logger.warning(
            "coordinates_not_found",
            external_id=external_id,
        )
        return {"latitude": 0.0, "longitude": 0.0}

    def _find_coordinates_in_html(
        self, html: str
    ) -> dict[str, float]:
        """Ищет координаты в HTML-коде страницы.

        Парсит HTML как текст, ищет паттерны с координатами
        в JSON-данных, вложенных в скрипты.

        Args:
            html: Полный HTML-код страницы.

        Returns:
            Словарь с latitude и longitude.
        """
        import re

        # Ищем паттерны координат Санкт-Петербурга (59.xx, 30.xx)
        # и Москвы (55.xx, 37.xx) и других городов РФ
        patterns = [
            r'"lat"\s*:\s*(\d{2}\.\d{3,8})\s*,\s*"lng"\s*:\s*(\d{2}\.\d{3,8})',
            r'"latitude"\s*:\s*(\d{2}\.\d{3,8})\s*,\s*"longitude"\s*:\s*(\d{2}\.\d{3,8})',
            r'data-map-lat="(\d{2}\.\d{3,8})"\s+data-map-lon="(\d{2}\.\d{3,8})"',
            r'"coords"\s*:\s*\[\s*(\d{2}\.\d{3,8})\s*,\s*(\d{2}\.\d{3,8})\s*\]',
        ]

        for pattern in patterns:
            match = re.search(pattern, html)
            if match:
                lat = float(match.group(1))
                lon = float(match.group(2))
                # Проверяем, что координаты в пределах России
                if 41.0 <= lat <= 82.0 and 19.0 <= lon <= 180.0:
                    return {"latitude": lat, "longitude": lon}

        return {"latitude": 0.0, "longitude": 0.0}

    async def _extract_calendar_data(
        self, page: Page, external_id: str
    ) -> dict:
        """Извлекает данные календаря и цен со страницы.

        Использует JavaScript-инъекцию для извлечения массива
        цен, календаря занятости, минимального срока аренды
        и флага мгновенного бронирования.

        Args:
            page: Активная страница Playwright.
            external_id: ID объявления для логирования.

        Returns:
            Словарь с ключами: prices, calendar, minStay, isInstantBook.
        """
        try:
            data = await page.evaluate(JS_EXTRACT_CALENDAR)
            logger.debug(
                "calendar_data_extracted",
                external_id=external_id,
                prices_count=len(data.get("prices", [])),
                calendar_count=len(data.get("calendar", [])),
                min_stay=data.get("minStay", 1),
                instant_book=data.get("isInstantBook", False),
            )
            return data
        except Exception as e:
            logger.warning(
                "calendar_extraction_failed",
                external_id=external_id,
                error=str(e),
            )
            return {
                "prices": [],
                "calendar": [],
                "minStay": 1,
                "isInstantBook": False,
            }

    async def _extract_room_category(
        self, page: Page, title: str
    ) -> RoomCategory:
        """Определяет категорию жилья по странице и названию.

        Сначала ищет категорию в параметрах объявления на странице,
        затем пытается определить по названию из каталога.

        Args:
            page: Активная страница Playwright.
            title: Название объявления из каталога.

        Returns:
            Категория жилья (RoomCategory enum).
        """
        # Способ 1: ищем в параметрах объявления на странице
        try:
            params_selector = (
                "[data-marker='item-view/item-params'],"
                "[data-marker='item-params']"
            )
            params_el = await page.query_selector(params_selector)
            if params_el:
                params_text = (await params_el.inner_text()).lower()
                category = self._match_room_category(params_text)
                if category != RoomCategory.UNKNOWN:
                    return category
        except Exception as e:
            logger.debug(
                "room_category_params_extraction_failed",
                error=str(e),
            )

        # Способ 2: определяем по названию из каталога
        category = self._match_room_category(title.lower())
        if category != RoomCategory.UNKNOWN:
            return category

        # Способ 3: ищем в хлебных крошках
        try:
            breadcrumbs_el = await page.query_selector(
                "[data-marker='breadcrumbs']"
            )
            if breadcrumbs_el:
                crumbs_text = (await breadcrumbs_el.inner_text()).lower()
                category = self._match_room_category(crumbs_text)
                if category != RoomCategory.UNKNOWN:
                    return category
        except Exception:
            pass

        return RoomCategory.UNKNOWN

    def _match_room_category(self, text: str) -> RoomCategory:
        """Сопоставляет текст с категорией жилья.

        Проверяет текст на наличие ключевых слов из маппинга
        категорий, начиная с наиболее специфичных.

        Args:
            text: Текст для анализа (нижний регистр).

        Returns:
            Найденная категория или UNKNOWN.
        """
        for keyword, category in ROOM_CATEGORY_MAP.items():
            if keyword in text:
                return category
        return RoomCategory.UNKNOWN

    async def _extract_host_rating(self, page: Page) -> float:
        """Извлекает рейтинг хоста со страницы объявления.

        Args:
            page: Активная страница Playwright.

        Returns:
            Рейтинг хоста (0.0 если не найден).
        """
        selectors = [
            "[data-marker='seller-rating/score']",
            "[data-marker='seller-info/rating']",
            "[class*='rating'] [class*='score']",
        ]

        for selector in selectors:
            try:
                el = await page.query_selector(selector)
                if el:
                    text = (await el.inner_text()).strip()
                    # Убираем всё кроме цифр и точки/запятой
                    cleaned = text.replace(",", ".").strip()
                    # Извлекаем первое число
                    import re
                    match = re.search(r"(\d+\.?\d*)", cleaned)
                    if match:
                        rating = float(match.group(1))
                        if 0.0 <= rating <= 5.0:
                            return rating
            except Exception:
                continue

        return 0.0

    async def _extract_last_update(
        self, page: Page
    ) -> datetime | None:
        """Извлекает время последнего обновления объявления хостом.

        Args:
            page: Активная страница Playwright.

        Returns:
            Datetime последнего обновления или None.
        """
        try:
            # Ищем дату обновления в информации об объявлении
            date_el = await page.query_selector(
                "[data-marker='item-view/item-date']"
            )
            if date_el:
                text = (await date_el.inner_text()).strip()
                parsed_dt = self._parse_russian_date(text)
                if parsed_dt:
                    return parsed_dt
        except Exception as e:
            logger.debug(
                "last_update_extraction_failed",
                error=str(e),
            )

        return None

    def _parse_russian_date(self, text: str) -> datetime | None:
        """Парсит дату в русском формате с Avito.

        Поддерживает форматы:
            - "сегодня в 14:30"
            - "вчера в 10:00"
            - "25 марта в 12:00"
            - "12 февраля 2025"

        Args:
            text: Текст с датой на русском языке.

        Returns:
            Объект datetime (UTC) или None если не удалось распарсить.
        """
        import re

        text = text.lower().strip()

        months_map = {
            "января": 1, "февраля": 2, "марта": 3,
            "апреля": 4, "мая": 5, "июня": 6,
            "июля": 7, "августа": 8, "сентября": 9,
            "октября": 10, "ноября": 11, "декабря": 12,
        }

        now = datetime.now(timezone.utc)

        # "сегодня в HH:MM"
        match = re.search(r"сегодня.*?(\d{1,2}):(\d{2})", text)
        if match:
            hour, minute = int(match.group(1)), int(match.group(2))
            return now.replace(
                hour=hour, minute=minute, second=0, microsecond=0
            )

        # "вчера в HH:MM"
        match = re.search(r"вчера.*?(\d{1,2}):(\d{2})", text)
        if match:
            from datetime import timedelta
            hour, minute = int(match.group(1)), int(match.group(2))
            yesterday = now - timedelta(days=1)
            return yesterday.replace(
                hour=hour, minute=minute, second=0, microsecond=0
            )

        # "DD месяц в HH:MM" или "DD месяц YYYY"
        for month_name, month_num in months_map.items():
            if month_name in text:
                day_match = re.search(
                    rf"(\d{{1,2}})\s+{month_name}", text
                )
                if day_match:
                    day = int(day_match.group(1))
                    year = now.year

                    year_match = re.search(r"(\d{4})", text)
                    if year_match:
                        year = int(year_match.group(1))

                    time_match = re.search(r"(\d{1,2}):(\d{2})", text)
                    hour = int(time_match.group(1)) if time_match else 0
                    minute = int(time_match.group(2)) if time_match else 0

                    try:
                        return datetime(
                            year, month_num, day,
                            hour, minute,
                            tzinfo=timezone.utc,
                        )
                    except ValueError:
                        return None

        return None

    def _pad_array(
        self, arr: list[int], target_length: int, fill_value: int
    ) -> list[int]:
        """Дополняет массив до заданной длины.

        Если массив короче target_length — дополняет fill_value.
        Если длиннее — обрезает.

        Args:
            arr: Исходный массив.
            target_length: Целевая длина.
            fill_value: Значение для заполнения.

        Returns:
            Массив ровно target_length элементов.
        """
        if len(arr) >= target_length:
            return arr[:target_length]
        return arr + [fill_value] * (target_length - len(arr))
