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
import re
from datetime import date, datetime, timezone

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

# Количество дней для сбора календаря и цен
CALENDAR_DAYS_TARGET: int = 60

# Таймаут ожидания появления datepicker после клика (мс)
DATEPICKER_OPEN_TIMEOUT: int = 5000

# Максимальное количество переключений месяцев в datepicker
MAX_MONTH_SWITCHES: int = 4

# Количество попыток разблокировки на странице карточки
MAX_LISTING_UNBLOCK_RETRIES: int = 5

# Ожидание между попытками разблокировки карточки (секунды)
LISTING_UNBLOCK_WAIT: int = 15

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

# CSS-селекторы для datepicker Avito
DATEPICKER_INPUT_SELECTOR: str = (
    "[data-marker='datepicker'] input,"
    "input[placeholder*='дата'],"
    "input[placeholder*='заезд'],"
    "input[placeholder*='Заезд'],"
    "div[data-marker*='date'] input"
)

# Контейнер datepicker (появляется после клика)
DATEPICKER_CONTAINER_SELECTOR: str = "[data-marker='datepicker']"

# Кнопка переключения на следующий месяц
DATEPICKER_NEXT_BUTTON_SELECTOR: str = (
    "[data-marker='datepicker/next-button']"
)

# Ячейка дня в datepicker (содержит данные о занятости)
DATEPICKER_DAY_CONTENT_SELECTOR: str = (
    "[data-marker='datepicker/content']"
)

# Маркеры доступности/занятости дня внутри ячейки
DATEPICKER_DAY_DISABLED_MARKER: str = "datepicker-day-disabled"
DATEPICKER_DAY_AVAILABLE_MARKER: str = "datepicker-day-available"


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
        is_instant_book: bool = False,
        catalog_host_rating: float = 0.0,
    ) -> RawListing | None:
        """Парсит одну карточку объявления аренды.

        Переходит на страницу объявления, извлекает все детальные
        данные и возвращает заполненный объект RawListing.
        При обнаружении блокировки — ожидает и повторяет
        до MAX_LISTING_UNBLOCK_RETRIES раз.

        Args:
            page: Активная страница Playwright.
            external_id: Идентификатор объявления (формат "av_<id>").
            url: Относительная или абсолютная ссылка на объявление.
            title: Название объявления из каталога.
            base_price: Базовая цена из каталога (руб./сут.).
            is_instant_book: Флаг мгновенного бронирования из каталога.
            catalog_host_rating: Рейтинг хоста из каталога (0.0 если
                не найден — будет извлечён со страницы карточки).

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

        # Проверяем блокировку с retry-логикой
        is_blocked = await self._browser_service._check_blocked()
        if is_blocked:
            unblocked = await self._wait_for_listing_unblock(
                page, external_id, full_url
            )
            if not unblocked:
                logger.warning(
                    "listing_page_permanently_blocked",
                    external_id=external_id,
                    max_attempts=MAX_LISTING_UNBLOCK_RETRIES,
                )
                return None

        # Извлекаем данные
        coordinates = await self._extract_coordinates(page, external_id)
        calendar_data = await self._extract_calendar_data(page, external_id)
        room_category = await self._extract_room_category(page, title)
        last_host_update = await self._extract_last_update(page)

        # Рейтинг хоста: приоритет — данные из каталога,
        # фоллбэк — поиск на странице карточки
        host_rating = catalog_host_rating
        if host_rating == 0.0:
            host_rating = await self._extract_host_rating(page)

        # Мгновенное бронирование: приоритет — бейдж из каталога,
        # фоллбэк — поиск на странице карточки
        instant_book = is_instant_book
        if not instant_book:
            instant_book = await self._extract_instant_book(
                page, external_id
            )

        # Формируем массив цен на 60 дней
        prices_60 = calendar_data.get("prices", [])
        if not prices_60:
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
            is_instant_book=instant_book,
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
            is_instant_book=instant_book,
            host_rating=host_rating,
        )

        return listing

    async def _wait_for_listing_unblock(
        self,
        page: Page,
        external_id: str,
        full_url: str,
    ) -> bool:
        """Ожидает снятия блокировки на странице карточки.

        Выполняет до MAX_LISTING_UNBLOCK_RETRIES попыток:
        пауза LISTING_UNBLOCK_WAIT секунд → обновление страницы →
        проверка блокировки.

        Args:
            page: Активная страница Playwright.
            external_id: ID объявления для логирования.
            full_url: Полный URL карточки объявления.

        Returns:
            True если блокировка снята.
        """
        for attempt in range(1, MAX_LISTING_UNBLOCK_RETRIES + 1):
            logger.warning(
                "listing_block_detected_waiting",
                external_id=external_id,
                attempt=attempt,
                max_attempts=MAX_LISTING_UNBLOCK_RETRIES,
                wait_seconds=LISTING_UNBLOCK_WAIT,
            )
            print(
                f"  [блокировка] Страница карточки заблокирована. "
                f"Попытка {attempt}/{MAX_LISTING_UNBLOCK_RETRIES}: "
                f"ожидание {LISTING_UNBLOCK_WAIT} сек..."
            )

            await asyncio.sleep(LISTING_UNBLOCK_WAIT)

            # Обновляем страницу
            try:
                await page.goto(
                    full_url,
                    wait_until="domcontentloaded",
                    timeout=60000,
                )
                await asyncio.sleep(random.uniform(3.0, 5.0))

                logger.info(
                    "listing_page_reloaded_after_block",
                    external_id=external_id,
                    attempt=attempt,
                )
                print(
                    f"  [блокировка] Страница обновлена, "
                    f"проверяю доступность..."
                )
            except Exception as e:
                logger.warning(
                    "listing_page_reload_failed",
                    external_id=external_id,
                    attempt=attempt,
                    error=str(e),
                )
                print(
                    f"  [блокировка] Ошибка обновления страницы: {e}"
                )
                continue

            # Проверяем, снята ли блокировка
            is_blocked = await self._browser_service._check_blocked()
            if not is_blocked:
                logger.info(
                    "listing_block_resolved",
                    external_id=external_id,
                    attempt=attempt,
                )
                print(
                    f"  [блокировка] Блокировка снята на попытке "
                    f"{attempt}!"
                )
                return True

            logger.warning(
                "listing_still_blocked",
                external_id=external_id,
                attempt=attempt,
            )
            print(
                f"  [блокировка] Всё ещё заблокировано "
                f"(попытка {attempt}/{MAX_LISTING_UNBLOCK_RETRIES})"
            )

        logger.error(
            "listing_block_not_resolved",
            external_id=external_id,
            max_attempts=MAX_LISTING_UNBLOCK_RETRIES,
            total_wait_seconds=MAX_LISTING_UNBLOCK_RETRIES * LISTING_UNBLOCK_WAIT,
        )
        print(
            f"  [блокировка] Не удалось разблокировать за "
            f"{MAX_LISTING_UNBLOCK_RETRIES} попыток "
            f"({MAX_LISTING_UNBLOCK_RETRIES * LISTING_UNBLOCK_WAIT} сек)"
        )
        return False

    # ==================================================================
    # Извлечение координат
    # ==================================================================

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

        Args:
            html: Полный HTML-код страницы.

        Returns:
            Словарь с latitude и longitude.
        """
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
                if 41.0 <= lat <= 82.0 and 19.0 <= lon <= 180.0:
                    return {"latitude": lat, "longitude": lon}

        return {"latitude": 0.0, "longitude": 0.0}

    # ==================================================================
    # Извлечение календаря занятости из DOM datepicker'а
    # ==================================================================

    async def _extract_calendar_data(
        self, page: Page, external_id: str
    ) -> dict:
        """Извлекает календарь занятости из datepicker на странице.

        Открывает datepicker кликом на поле даты, затем парсит
        видимые месяцы. Определяет занятость каждого дня по
        наличию атрибута data-disabled="true" на элементах
        с data-marker="datepicker/content". Листает месяцы
        вперёд кнопкой next для сбора 60 дней.

        Важно: дни раньше сегодняшней даты отфильтровываются,
        чтобы массив calendar начинался строго с текущего дня.

        Args:
            page: Активная страница Playwright.
            external_id: ID объявления для логирования.

        Returns:
            Словарь с ключами: prices, calendar, minStay.
        """
        result: dict = {
            "prices": [],
            "calendar": [],
            "minStay": 1,
        }

        # --- Шаг 1: Открываем datepicker ---
        datepicker_opened = await self._open_datepicker(
            page, external_id
        )
        if not datepicker_opened:
            logger.warning(
                "datepicker_not_opened",
                external_id=external_id,
            )
            return result

        # --- Шаг 2: Парсим видимые месяцы и листаем вперёд ---
        today = date.today()
        all_days: list[dict] = []
        months_switched = 0

        while len(all_days) < CALENDAR_DAYS_TARGET + 31:
            new_days = await self._parse_visible_calendar_days_raw(
                page, external_id
            )

            if not new_days:
                logger.debug(
                    "datepicker_no_days_on_visible_months",
                    external_id=external_id,
                    months_switched=months_switched,
                    total_days=len(all_days),
                )
                break

            all_days.extend(new_days)

            logger.debug(
                "datepicker_days_collected",
                external_id=external_id,
                new_days=len(new_days),
                total_days=len(all_days),
                months_switched=months_switched,
            )

            if months_switched >= MAX_MONTH_SWITCHES:
                logger.debug(
                    "datepicker_max_month_switches_reached",
                    external_id=external_id,
                    max_switches=MAX_MONTH_SWITCHES,
                    total_days=len(all_days),
                )
                break

            switched = await self._click_next_month(page, external_id)
            if not switched:
                logger.debug(
                    "datepicker_cannot_switch_month",
                    external_id=external_id,
                    months_switched=months_switched,
                )
                break

            months_switched += 1
            await asyncio.sleep(random.uniform(0.5, 1.0))

        # --- Шаг 2.1: Фильтрация — только дни начиная с сегодня ---
        filtered_bits: list[int] = []
        for day_info in all_days:
            try:
                day_date = date(
                    day_info["year"],
                    day_info["month"],
                    day_info["day"],
                )
            except (ValueError, KeyError):
                continue

            if day_date < today:
                continue

            if day_info.get("disabled", False):
                filtered_bits.append(1)
            else:
                filtered_bits.append(0)

            if len(filtered_bits) >= CALENDAR_DAYS_TARGET:
                break

        result["calendar"] = filtered_bits[:CALENDAR_DAYS_TARGET]

        logger.info(
            "calendar_extracted_from_datepicker",
            external_id=external_id,
            total_days=len(result["calendar"]),
            occupied_days=sum(result["calendar"]),
            months_switched=months_switched,
            first_date=str(today),
        )

        # --- Шаг 3: Извлекаем мин. срок ---
        result["minStay"] = await self._extract_min_stay(
            page, external_id
        )

        # Закрываем datepicker
        await self._close_datepicker(page)

        return result

    async def _open_datepicker(
        self, page: Page, external_id: str
    ) -> bool:
        """Открывает datepicker кликом на поле ввода даты.

        Пробует несколько CSS-селекторов для поиска поля даты,
        затем кликает и ожидает появления контейнера datepicker.

        Args:
            page: Активная страница Playwright.
            external_id: ID объявления для логирования.

        Returns:
            True если datepicker успешно открылся.
        """
        existing = await page.query_selector(
            DATEPICKER_CONTAINER_SELECTOR
        )
        if existing:
            logger.debug(
                "datepicker_already_visible",
                external_id=external_id,
            )
            return True

        input_selectors = [
            "[data-marker='datepicker'] input",
            "input[placeholder*='Заезд']",
            "input[placeholder*='заезд']",
            "input[placeholder*='дата']",
            "input[placeholder*='Дата']",
            "div[data-marker*='date'] input",
            "[data-marker*='booking'] input",
            "[data-marker*='calendar'] input",
        ]

        for selector in input_selectors:
            try:
                input_el = await page.query_selector(selector)
                if input_el:
                    await input_el.click()
                    logger.debug(
                        "datepicker_input_clicked",
                        external_id=external_id,
                        selector=selector,
                    )

                    try:
                        await page.wait_for_selector(
                            DATEPICKER_CONTAINER_SELECTOR,
                            timeout=DATEPICKER_OPEN_TIMEOUT,
                        )
                        logger.debug(
                            "datepicker_opened_successfully",
                            external_id=external_id,
                            selector=selector,
                        )
                        return True
                    except Exception:
                        logger.debug(
                            "datepicker_not_appeared_after_click",
                            external_id=external_id,
                            selector=selector,
                        )
                        continue
            except Exception as e:
                logger.debug(
                    "datepicker_input_click_failed",
                    external_id=external_id,
                    selector=selector,
                    error=str(e),
                )
                continue

        try:
            fallback_input = await page.query_selector(
                "div._58fc8f170622acf7 input"
            )
            if fallback_input:
                await fallback_input.click()
                try:
                    await page.wait_for_selector(
                        DATEPICKER_CONTAINER_SELECTOR,
                        timeout=DATEPICKER_OPEN_TIMEOUT,
                    )
                    logger.debug(
                        "datepicker_opened_via_fallback",
                        external_id=external_id,
                    )
                    return True
                except Exception:
                    pass
        except Exception:
            pass

        logger.warning(
            "datepicker_input_not_found",
            external_id=external_id,
        )
        return False

    async def _parse_visible_calendar_days_raw(
        self, page: Page, external_id: str
    ) -> list[dict]:
        """Парсит все видимые дни из открытого datepicker.

        Возвращает сырые данные с годом, месяцем, днём и
        флагом disabled для каждого дня. Фильтрация по дате
        выполняется вызывающим кодом.

        Args:
            page: Активная страница Playwright.
            external_id: ID объявления для логирования.

        Returns:
            Список словарей с ключами year, month, day, disabled.
            Месяц — 1-indexed (1=январь, 12=декабрь).
        """
        js_parse_days = """
        () => {
            const datepicker = document.querySelector(
                '[data-marker="datepicker"]'
            );
            if (!datepicker) return [];

            const calendars = datepicker.querySelectorAll(
                '[data-marker^="datepicker/calendar("]'
            );
            const results = [];

            for (const calendar of calendars) {
                const marker = calendar.getAttribute('data-marker') || '';
                const monthMatch = marker.match(
                    /calendar\\((\\d{4})-(\\d{1,2})\\)/
                );
                const calYear = monthMatch ? parseInt(monthMatch[1]) : 0;
                const calMonth = monthMatch
                    ? parseInt(monthMatch[2]) + 1
                    : 0;

                const dayElements = calendar.querySelectorAll(
                    '[data-marker="datepicker/content"]'
                );

                for (const dayEl of dayElements) {
                    const innerDiv = dayEl.querySelector(
                        '[data-marker="datepicker-day-disabled"],'
                        + '[data-marker="datepicker-day-available"]'
                    );

                    if (!innerDiv) continue;

                    const dayText = innerDiv.textContent.trim();
                    const dayNum = parseInt(dayText);
                    if (isNaN(dayNum)) continue;

                    const isDisabled =
                        dayEl.getAttribute('data-disabled') === 'true';

                    results.push({
                        year: calYear,
                        month: calMonth,
                        day: dayNum,
                        disabled: isDisabled
                    });
                }
            }

            return results;
        }
        """

        try:
            raw_days = await page.evaluate(js_parse_days)
        except Exception as e:
            logger.warning(
                "datepicker_js_parse_failed",
                external_id=external_id,
                error=str(e),
            )
            return []

        if not raw_days:
            return []

        try:
            sorted_days = sorted(
                raw_days,
                key=lambda d: (d["year"], d["month"], d["day"]),
            )
        except (KeyError, TypeError):
            sorted_days = raw_days

        logger.debug(
            "datepicker_visible_days_parsed_raw",
            external_id=external_id,
            total_days=len(sorted_days),
            first_day=(
                f"{sorted_days[0]['year']}-"
                f"{sorted_days[0]['month']:02d}-"
                f"{sorted_days[0]['day']:02d}"
                if sorted_days else "none"
            ),
            last_day=(
                f"{sorted_days[-1]['year']}-"
                f"{sorted_days[-1]['month']:02d}-"
                f"{sorted_days[-1]['day']:02d}"
                if sorted_days else "none"
            ),
        )

        return sorted_days

    async def _click_next_month(
        self, page: Page, external_id: str
    ) -> bool:
        """Нажимает кнопку переключения на следующий месяц в datepicker.

        Args:
            page: Активная страница Playwright.
            external_id: ID объявления для логирования.

        Returns:
            True если кнопка найдена и нажата успешно.
        """
        try:
            next_button = await page.query_selector(
                DATEPICKER_NEXT_BUTTON_SELECTOR
            )
            if not next_button:
                logger.debug(
                    "datepicker_next_button_not_found",
                    external_id=external_id,
                )
                return False

            is_disabled = await next_button.get_attribute("disabled")
            if is_disabled is not None:
                logger.debug(
                    "datepicker_next_button_disabled",
                    external_id=external_id,
                )
                return False

            await next_button.click()
            await asyncio.sleep(random.uniform(0.5, 1.0))

            logger.debug(
                "datepicker_next_month_clicked",
                external_id=external_id,
            )
            return True

        except Exception as e:
            logger.warning(
                "datepicker_next_month_click_failed",
                external_id=external_id,
                error=str(e),
            )
            return False

    async def _extract_min_stay(
        self, page: Page, external_id: str
    ) -> int:
        """Извлекает минимальный срок аренды из datepicker.

        Для отображения текста «Бронь минимум от N ночей»
        нужно кликнуть на свободную дату в datepicker.
        После извлечения данных закрывает и переоткрывает
        datepicker для возврата в исходное состояние.

        Приоритет источников:
        1. Текст внутри datepicker после клика на свободную дату.
        2. Текст на странице карточки объявления.
        3. Данные из window.__initialData__.

        Args:
            page: Активная страница Playwright.
            external_id: ID объявления для логирования.

        Returns:
            Минимальный срок в сутках (1 по умолчанию).
        """
        try:
            datepicker_el = await page.query_selector(
                DATEPICKER_CONTAINER_SELECTOR
            )
            if datepicker_el:
                min_stay = await self._click_free_day_and_read_min_stay(
                    page, external_id
                )
                if min_stay is not None:
                    return min_stay

            page_text = await page.evaluate(
                "document.body.innerText"
            )
            patterns = [
                r"(?:минимум|мин\.?)\s*от\s*(\d+)\s*"
                r"(?:ноч[еёий]*|суток|сут\.|дн[яей]*)",
                r"(?:от|мин[.\s]*срок[а]?)\s*(\d+)\s*"
                r"(?:суток|сут\.|дн[яей]*|ноч[еёий]*)",
                r"минимальн\w*\s*(?:срок\w*)?\s*(\d+)\s*"
                r"(?:суток|сут\.|дн[яей]*|ноч[еёий]*)",
                r"(\d+)\s*(?:суток|сут\.)\s*(?:минимум|мин\.?)",
            ]
            for pattern in patterns:
                match = re.search(pattern, page_text, re.IGNORECASE)
                if match:
                    min_stay_val = int(match.group(1))
                    if 1 <= min_stay_val <= 365:
                        logger.debug(
                            "min_stay_extracted_from_page_text",
                            external_id=external_id,
                            min_stay=min_stay_val,
                        )
                        return min_stay_val

            js_min_stay = """
            () => {
                try {
                    const data = window.__initialData__;
                    if (data) {
                        const text = JSON.stringify(data);
                        const match = text.match(/"minDays":\\s*(\\d+)/);
                        if (match) return parseInt(match[1]);
                    }
                } catch (e) {}
                return 0;
            }
            """
            result = await page.evaluate(js_min_stay)
            if result and result > 0:
                logger.debug(
                    "min_stay_extracted_from_initial_data",
                    external_id=external_id,
                    min_stay=result,
                )
                return result

        except Exception as e:
            logger.debug(
                "min_stay_extraction_failed",
                external_id=external_id,
                error=str(e),
            )

        return 1

    async def _click_free_day_and_read_min_stay(
        self, page: Page, external_id: str
    ) -> int | None:
        """Кликает на свободные даты и читает мин. срок.

        После каждого клика на дату Avito активирует режим выбора
        диапазона (заезд → выезд), из-за чего DOM datepicker
        перестраивается. Поэтому после каждой попытки datepicker
        закрывается и открывается заново, а свободные даты
        находятся повторно из свежего DOM.

        Args:
            page: Активная страница Playwright.
            external_id: ID объявления для логирования.

        Returns:
            Минимальный срок в сутках или None если не удалось.
        """
        max_attempts = 5
        day_indices_to_try = [0, 1, 3, 5, 8]

        for attempt_num, day_index in enumerate(
            day_indices_to_try[:max_attempts], start=1
        ):
            print(
                f"\n  [datepicker] Попытка {attempt_num}/{max_attempts}: "
                f"ищу свободную дату с индексом {day_index}"
            )

            datepicker_visible = await page.query_selector(
                DATEPICKER_CONTAINER_SELECTOR
            )
            if not datepicker_visible:
                reopened = await self._open_datepicker(page, external_id)
                if not reopened:
                    print(
                        f"  [datepicker] Не удалось открыть datepicker "
                        f"на попытке {attempt_num}"
                    )
                    return None
                await asyncio.sleep(0.5)

            free_day_elements = await page.query_selector_all(
                "[data-marker='datepicker'] "
                "[data-marker='datepicker-day-available']"
            )

            if not free_day_elements:
                print(
                    f"  [datepicker] Свободные даты не найдены "
                    f"на попытке {attempt_num}"
                )
                return None

            total_free = len(free_day_elements)
            print(
                f"  [datepicker] Свободных дат в DOM: {total_free}"
            )

            actual_index = min(day_index, total_free - 1)
            day_el = free_day_elements[actual_index]

            try:
                day_text = (await day_el.inner_text()).strip()
            except Exception:
                day_text = f"#{actual_index}"

            print(
                f"  [datepicker] Кликаю на день {day_text} "
                f"(индекс {actual_index})"
            )

            try:
                await day_el.click()
            except Exception as e:
                print(
                    f"  [datepicker] Не удалось кликнуть "
                    f"на день {day_text}: {e}"
                )
                await self._close_datepicker(page)
                await asyncio.sleep(0.5)
                continue

            await asyncio.sleep(1.0)

            datepicker_el = await page.query_selector(
                DATEPICKER_CONTAINER_SELECTOR
            )
            if not datepicker_el:
                print(
                    f"  [datepicker] Контейнер datepicker исчез "
                    f"после клика на день {day_text}"
                )
                await asyncio.sleep(0.5)
                continue

            dp_text = (await datepicker_el.inner_text()).strip()

            print(
                f"  [datepicker] Текст datepicker после клика "
                f"на день {day_text}:\n"
                f"  ---\n"
                f"  {dp_text}\n"
                f"  ---"
            )

            logger.debug(
                "min_stay_datepicker_text_after_click",
                external_id=external_id,
                day=day_text,
                attempt=attempt_num,
                text=dp_text[:200],
            )

            dp_match = re.search(
                r"(?:бронь|минимум|мин\.?)\s*(?:от\s*)?(\d+)\s*"
                r"(?:ноч[еёий]*|суток|сут\.|дн[яей]*)",
                dp_text,
                re.IGNORECASE,
            )
            if dp_match:
                min_stay_val = int(dp_match.group(1))
                if 1 <= min_stay_val <= 365:
                    print(
                        f"  [datepicker] Найден мин. срок: "
                        f"{min_stay_val} сут. (день {day_text})"
                    )
                    logger.debug(
                        "min_stay_extracted_from_datepicker",
                        external_id=external_id,
                        min_stay=min_stay_val,
                        day=day_text,
                        attempt=attempt_num,
                    )
                    await self._close_datepicker(page)
                    return min_stay_val
            else:
                print(
                    f"  [datepicker] Паттерн мин. срока "
                    f"НЕ найден (день {day_text})"
                )

            await self._close_datepicker(page)
            await asyncio.sleep(0.5)

        print(
            f"  [datepicker] Мин. срок не найден ни в одной "
            f"из {max_attempts} попыток"
        )
        return None

    async def _close_datepicker(self, page: Page) -> None:
        """Закрывает datepicker кликом вне его области.

        Args:
            page: Активная страница Playwright.
        """
        try:
            buttons = await page.query_selector_all(
                "[data-marker='datepicker'] button"
            )
            for button in buttons:
                try:
                    button_text = (await button.inner_text()).strip()
                    if "сбросить" in button_text.lower():
                        await button.click()
                        await asyncio.sleep(0.5)
                        print(
                            f"  [datepicker] Нажата кнопка «Сбросить»"
                        )
                        break
                except Exception:
                    continue
        except Exception:
            pass

        try:
            await page.click("body", position={"x": 10, "y": 10})
            await asyncio.sleep(0.3)
        except Exception:
            pass

        try:
            await page.keyboard.press("Escape")
            await asyncio.sleep(0.3)
        except Exception:
            pass

    async def _extract_instant_book(
        self, page: Page, external_id: str
    ) -> bool:
        """Извлекает флаг мгновенного бронирования со страницы карточки.

        Используется как фоллбэк, если бейдж не был найден
        в каталоге. Ищет признаки мгновенного бронирования
        в DOM и в тексте страницы.

        Args:
            page: Активная страница Playwright.
            external_id: ID объявления для логирования.

        Returns:
            True если мгновенное бронирование доступно.
        """
        try:
            instant_el = await page.query_selector(
                "[data-marker*='instant-booking'],"
                "[data-marker*='instantBooking']"
            )
            if instant_el:
                return True

            page_text = await page.evaluate(
                "document.body.innerText"
            )
            instant_phrases = [
                "Мгновенное бронирование",
                "Мгновенная бронь",
                "Забронировать сейчас",
                "Моментальное бронирование",
            ]
            for phrase in instant_phrases:
                if phrase in page_text:
                    return True

            js_instant = """
            () => {
                try {
                    const data = window.__initialData__;
                    if (data) {
                        const text = JSON.stringify(data);
                        if (text.includes('"instantBooking":true')
                            || text.includes('"isInstantBooking":true')) {
                            return true;
                        }
                    }
                } catch (e) {}
                return false;
            }
            """
            result = await page.evaluate(js_instant)
            if result:
                return True

        except Exception as e:
            logger.debug(
                "instant_book_extraction_failed",
                external_id=external_id,
                error=str(e),
            )

        return False

    # ==================================================================
    # Извлечение категории жилья
    # ==================================================================

    async def _extract_room_category(
        self, page: Page, title: str
    ) -> RoomCategory:
        """Определяет категорию жилья по странице и названию.

        Args:
            page: Активная страница Playwright.
            title: Название объявления из каталога.

        Returns:
            Категория жилья (RoomCategory enum).
        """
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

        category = self._match_room_category(title.lower())
        if category != RoomCategory.UNKNOWN:
            return category

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

        Args:
            text: Текст для анализа (нижний регистр).

        Returns:
            Найденная категория или UNKNOWN.
        """
        for keyword, category in ROOM_CATEGORY_MAP.items():
            if keyword in text:
                return category
        return RoomCategory.UNKNOWN

    # ==================================================================
    # Извлечение рейтинга хоста
    # ==================================================================

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
                    cleaned = text.replace(",", ".").strip()
                    match = re.search(r"(\d+\.?\d*)", cleaned)
                    if match:
                        rating = float(match.group(1))
                        if 0.0 <= rating <= 5.0:
                            return rating
            except Exception:
                continue

        return 0.0

    # ==================================================================
    # Извлечение даты обновления
    # ==================================================================

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

        Args:
            text: Текст с датой на русском языке.

        Returns:
            Объект datetime (UTC) или None.
        """
        text = text.lower().strip()

        months_map = {
            "января": 1, "февраля": 2, "марта": 3,
            "апреля": 4, "мая": 5, "июня": 6,
            "июля": 7, "августа": 8, "сентября": 9,
            "октября": 10, "ноября": 11, "декабря": 12,
        }

        now = datetime.now(timezone.utc)

        match = re.search(r"сегодня.*?(\d{1,2}):(\d{2})", text)
        if match:
            hour, minute = int(match.group(1)), int(match.group(2))
            return now.replace(
                hour=hour, minute=minute, second=0, microsecond=0
            )

        match = re.search(r"вчера.*?(\d{1,2}):(\d{2})", text)
        if match:
            from datetime import timedelta
            hour, minute = int(match.group(1)), int(match.group(2))
            yesterday = now - timedelta(days=1)
            return yesterday.replace(
                hour=hour, minute=minute, second=0, microsecond=0
            )

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

    # ==================================================================
    # Утилиты
    # ==================================================================

    def _pad_array(
        self, arr: list[int], target_length: int, fill_value: int
    ) -> list[int]:
        """Дополняет массив до заданной длины.

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
