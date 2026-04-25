"""Сервис парсинга карточки объявления краткосрочной аренды.

Заходит на страницу конкретного объявления Avito и извлекает
детальные данные: координаты, календарь занятости, цены на
60 дней вперёд, условия аренды и информацию о хосте.

При обнаружении блокировки — сообщает трекеру здоровья прокси
о бане (через BrowserService.report_ban()), при успехе —
об успехе (BrowserService.report_success()). Это позволяет
трекеру автоматически исключать «мёртвые» прокси из пула.

Паттерн Strategy: при переходе на другую площадку (Суточно.ру)
можно создать альтернативную реализацию с тем же интерфейсом.
"""

import asyncio
import json
import random
import re
from datetime import date, datetime, timedelta, timezone
from urllib.parse import urlparse, urlunparse

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

# Максимальное количество смен прокси для одной карточки
# (защита от бесконечного цикла ротации)
MAX_PROXY_ROTATIONS_PER_LISTING: int = 3

# Минимальный срок аренды по умолчанию, если не удалось прочитать
# из датепикера (выбранная дата + 1 день = 2 суток)
DEFAULT_MIN_STAY_FALLBACK: int = 2

# Количество попыток перезагрузки страницы при отсутствии календаря
MAX_CALENDAR_RELOAD_RETRIES: int = 3

# Ожидание перед перезагрузкой страницы при отсутствии календаря (секунды)
CALENDAR_RELOAD_WAIT_SECONDS: int = 10

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

# CSS-селектор кнопки «Сбросить» в датепикере (основной)
DATEPICKER_RESET_BUTTON_SELECTOR: str = (
    "button._8761af61d40d8964.f6eebfeb30fe503c._793efba06309a0ff"
)
# Селектор карусели ближайших дат (появляется после выбора заезда и выезда)
NEAREST_DATES_SELECTOR: str = "ul[role='listbox'][data-marker='nearest-dates']"

# Максимальное количество повторных попыток выбора дат при отсутствии карусели
MAX_DATE_SELECTION_RETRIES: int = 3

# Таймаут ожидания появления карусели ближайших дат (мс)
NEAREST_DATES_TIMEOUT: int = 3000


def _clean_listing_url(url: str) -> str:
    """Очищает URL карточки от query-параметров бронирования.

    Avito добавляет к ссылкам в каталоге параметры сессии
    бронирования (checkIn, checkOut, guests, context и др.),
    которые вызывают ERR_HTTP_RESPONSE_CODE_FAILURE при
    переходе из другого контекста.

    Args:
        url: URL карточки (относительный или абсолютный).

    Returns:
        Чистый URL без query-параметров.
    """
    parsed = urlparse(url)
    clean = urlunparse((
        parsed.scheme,
        parsed.netloc,
        parsed.path,
        "",
        "",
        "",
    ))
    if clean != url:
        logger.debug(
            "listing_url_cleaned",
            original_length=len(url),
            clean_url=clean[:200],
        )
    return clean


class ListingService:
    """Сервис для парсинга карточки объявления краткосрочной аренды.

    Переходит на страницу конкретного объявления и извлекает
    все детальные данные, недоступные из каталога: координаты,
    календарь, цены на 60 дней, условия аренды.

    При обнаружении блокировки — сообщает трекеру здоровья через
    BrowserService.report_ban(). При успешном парсинге — вызывает
    BrowserService.report_success(). Это позволяет трекеру
    автоматически исключать прокси с высоким процентом банов.

    При стандартных retry исчерпаны и доступны прокси — выполняет
    ротацию прокси и пробует снова с новым IP.

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
        данные, затем проходит по каждому свободному дню и через
        датепикер получает реальную цену за сутки.

        При успешном парсинге сообщает трекеру здоровья об успехе
        текущего прокси. При провале (календарь не загрузился) —
        не сообщает (бан уже зарегистрирован в _navigate_to_listing
        или _retry_with_proxy_rotation).

        Если календарь (datepicker) не удалось загрузить после
        всех попыток перезагрузки — карточка считается проваленной
        и возвращается None. Мусорные данные (все дни заняты,
        все цены нулевые) в БД НЕ записываются.

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
            Заполненный RawListing или None при ошибке парсинга
            (включая случай, когда календарь не загрузился).
        """
        # Очищаем URL от query-параметров бронирования
        clean_url = _clean_listing_url(url)
        full_url = (
            clean_url if clean_url.startswith("http")
            else f"https://www.avito.ru{clean_url}"
        )

        # Используем текущую page из browser_service
        current_page = self._browser_service.page or page

        # Попытка навигации на карточку с обработкой блокировки
        navigated = await self._navigate_to_listing(
            current_page, external_id, full_url
        )

        if not navigated:
            current_page = await self._retry_with_proxy_rotation(
                external_id, full_url
            )
            if current_page is None:
                logger.warning(
                    "listing_page_permanently_blocked",
                    external_id=external_id,
                    max_proxy_rotations=MAX_PROXY_ROTATIONS_PER_LISTING,
                )
                return None

        # После возможной ротации берём актуальную page
        current_page = self._browser_service.page or current_page

        # Извлекаем данные
        coordinates = await self._extract_coordinates(
            current_page, external_id
        )
        calendar_data = await self._extract_calendar_data(
            current_page, external_id, full_url
        )
        room_category = await self._extract_room_category(
            current_page, title
        )
        last_host_update = await self._extract_last_update(current_page)

        host_rating = catalog_host_rating
        if host_rating == 0.0:
            host_rating = await self._extract_host_rating(current_page)

        instant_book = is_instant_book
        if not instant_book:
            instant_book = await self._extract_instant_book(
                current_page, external_id
            )

        # Проверяем, удалось ли извлечь календарь.
        # Если calendar пустой — datepicker так и не загрузился:
        # карточка считается проваленной, возвращаем None.
        # Мусорные данные (все дни заняты, все цены = 0)
        # в БД НЕ записываются — карточка попадёт в failover.
        raw_calendar = calendar_data.get("calendar", [])
        calendar_failed = len(raw_calendar) == 0

        if calendar_failed:
            logger.warning(
                "calendar_empty_listing_failed",
                external_id=external_id,
                reason="datepicker не загрузился после всех попыток",
            )
            print(
                f"  [календарь] {external_id}: календарь не получен — "
                f"карточка считается проваленной (будет повторена "
                f"с другим прокси)."
            )
            return None

        calendar_60 = self._pad_array(raw_calendar, 60, 0)
        min_stay = calendar_data.get("minStay", 1)

        # === Извлечение реальных цен через датепикер ===
        prices_60 = await self._extract_prices_for_free_days(
            current_page,
            external_id,
            calendar_60,
            min_stay,
            base_price,
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
            min_stay=min_stay,
            is_instant_book=instant_book,
            host_rating=host_rating,
            url=url,
            title=title,
        )

        # Успешный парсинг — сообщаем трекеру
        self._browser_service.report_success()

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

    # ==================================================================
    # Извлечение реальных цен через датепикер
    # ==================================================================

    async def _extract_prices_for_free_days(
        self,
        page: Page,
        external_id: str,
        calendar_60: list[int],
        min_stay: int,
        base_price: int,
    ) -> list[int]:
        """Извлекает реальные цены для каждого свободного дня.

        Обходит дни последовательно (0..59). Для каждого свободного
        дня, который ещё не был обработан:
        1. Открывает датепикер, сбрасывает состояние.
        2. Листает до месяца заезда, кликает дату заезда.
        3. Читает мин. срок из текста датепикера.
        4. Вычисляет дату выезда = заезд + мин. срок.
        5. Листает до месяца выезда, кликает дату выезда.
        6. Проверяет карусель nearest-dates → читает цену.
        7. «Разблокирует» следующие (мин. срок − 1) дней:
           если день был занят (calendar_60[j] == 1) — меняет
           на 0 (свободен), назначает ту же цену и добавляет
           в множество обработанных (пропустится при обходе).
           Если день уже был свободен — не трогает его,
           он будет обработан отдельно со своей ценой.

        Занятые дни, не разблокированные мин. сроком, получают
        цену 0.

        Args:
            page: Активная страница Playwright.
            external_id: ID объявления для логирования.
            calendar_60: Массив занятости (0 — свободен, 1 — занят).
                         Мутируется: разблокированные дни меняются
                         с 1 на 0.
            min_stay: Фоллбэк мин. срока аренды из календаря.
            base_price: Цена-фоллбэк при ошибке чтения.

        Returns:
            Массив цен на 60 дней.
        """
        today = date.today()
        prices: list[int] = [0] * CALENDAR_DAYS_TARGET

        # Множество индексов дней, которые уже обработаны
        # (разблокированы через мин. срок предыдущего свободного дня).
        # Эти дни пропускаются при обходе — их цена уже назначена.
        processed_by_min_stay: set[int] = set()

        initial_free_count = sum(
            1 for i in range(len(calendar_60)) if calendar_60[i] == 0
        )

        if initial_free_count == 0:
            logger.info(
                "prices_no_free_days",
                external_id=external_id,
            )
            return prices

        logger.info(
            "prices_extraction_started",
            external_id=external_id,
            free_days=initial_free_count,
            total_days=len(calendar_60),
        )

        for day_idx in range(CALENDAR_DAYS_TARGET):
            # Пропускаем занятые дни (не разблокированные)
            if calendar_60[day_idx] == 1:
                continue

            # Пропускаем дни, уже обработанные через мин. срок
            # предыдущего свободного дня (были заняты → разблокированы)
            if day_idx in processed_by_min_stay:
                continue

            checkin = today + timedelta(days=day_idx)
            day_price: int | None = None
            actual_min_stay: int = DEFAULT_MIN_STAY_FALLBACK

            for attempt in range(1, MAX_DATE_SELECTION_RETRIES + 1):
                # --- Шаг 1: Закрываем датепикер если открыт ---
                existing_dp = await page.query_selector(
                    DATEPICKER_CONTAINER_SELECTOR
                )
                if existing_dp:
                    await self._close_datepicker(page)
                    await asyncio.sleep(0.3)

                # --- Шаг 2: Открываем датепикер заново ---
                opened = await self._open_datepicker(page, external_id)
                if not opened:
                    logger.debug(
                        "prices_datepicker_not_opened",
                        external_id=external_id,
                        day_idx=day_idx,
                        attempt=attempt,
                    )
                    break

                await asyncio.sleep(0.3)

                # --- Шаг 3: Сбрасываем датепикер кнопкой «Сбросить» ---
                await self._reset_datepicker(page, external_id)
                await asyncio.sleep(0.3)

                # --- Шаг 4: Листаем до месяца заезда ---
                month_found = await self._navigate_datepicker_to_month(
                    page, checkin.year, checkin.month
                )
                if not month_found:
                    logger.debug(
                        "prices_month_not_found",
                        external_id=external_id,
                        month=f"{checkin.year}-{checkin.month:02d}",
                        attempt=attempt,
                    )
                    await self._close_datepicker(page)
                    break

                # --- Шаг 5: Кликаем на дату заезда ---
                clicked_in = await self._click_datepicker_day(
                    page, checkin.year, checkin.month, checkin.day
                )
                if not clicked_in:
                    logger.debug(
                        "prices_checkin_click_failed",
                        external_id=external_id,
                        checkin=str(checkin),
                        attempt=attempt,
                    )
                    await self._close_datepicker(page)
                    continue

                await asyncio.sleep(0.5)

                # --- Шаг 6: Читаем мин. срок из текста датепикера ---
                actual_min_stay = (
                    await self._read_min_stay_from_datepicker(
                        page, DEFAULT_MIN_STAY_FALLBACK
                    )
                )
                checkout = checkin + timedelta(days=actual_min_stay)

                # --- Шаг 7: Листаем до месяца выезда и кликаем ---
                checkout_month_found = (
                    await self._navigate_datepicker_to_month(
                        page, checkout.year, checkout.month
                    )
                )
                if checkout_month_found:
                    clicked_out = await self._click_datepicker_day(
                        page, checkout.year, checkout.month, checkout.day
                    )
                    if not clicked_out:
                        logger.debug(
                            "prices_checkout_click_failed",
                            external_id=external_id,
                            checkout=str(checkout),
                            attempt=attempt,
                        )
                    await asyncio.sleep(0.5)

                # --- Шаг 8: Проверяем появление карусели ---
                carousel_appeared = (
                    await self._check_nearest_dates_appeared(page)
                )

                if carousel_appeared:
                    # Клики сработали — читаем цену
                    price = await self._read_item_price(page)
                    if price is not None and price > 0:
                        day_price = price
                    logger.debug(
                        "prices_day_extracted",
                        external_id=external_id,
                        day_idx=day_idx,
                        checkin=str(checkin),
                        checkout=str(checkout),
                        min_stay_used=actual_min_stay,
                        price=day_price or base_price,
                        attempt=attempt,
                    )
                    break

                # Карусель не появилась — клики не сработали
                logger.debug(
                    "prices_nearest_dates_not_found",
                    external_id=external_id,
                    day_idx=day_idx,
                    checkin=str(checkin),
                    checkout=str(checkout),
                    attempt=attempt,
                    max_attempts=MAX_DATE_SELECTION_RETRIES,
                )

                await self._close_datepicker(page)
                await asyncio.sleep(0.5)

            # Записываем итоговую цену для текущего дня
            resolved_price = (
                day_price if day_price is not None and day_price > 0
                else base_price
            )
            prices[day_idx] = resolved_price

            # === Разблокировка дней по минимальному сроку ===
            for offset in range(1, actual_min_stay):
                unlock_idx = day_idx + offset
                if unlock_idx >= CALENDAR_DAYS_TARGET:
                    break

                if calendar_60[unlock_idx] == 1:
                    # День был занят → разблокируем
                    calendar_60[unlock_idx] = 0
                    prices[unlock_idx] = resolved_price
                    processed_by_min_stay.add(unlock_idx)

                    logger.debug(
                        "day_unlocked_by_min_stay",
                        external_id=external_id,
                        source_day_idx=day_idx,
                        unlocked_day_idx=unlock_idx,
                        min_stay=actual_min_stay,
                        assigned_price=resolved_price,
                    )

            # --- Финальный сброс датепикера ---
            await self._close_datepicker(page)
            await asyncio.sleep(0.3)

        # Итоговая статистика
        final_free_count = sum(
            1 for i in range(len(calendar_60)) if calendar_60[i] == 0
        )
        unlocked_count = len(processed_by_min_stay)
        filled = sum(
            1 for i in range(CALENDAR_DAYS_TARGET)
            if calendar_60[i] == 0 and prices[i] > 0
        )

        logger.info(
            "prices_extraction_completed",
            external_id=external_id,
            initial_free_days=initial_free_count,
            unlocked_days=unlocked_count,
            final_free_days=final_free_count,
            prices_filled=filled,
        )

        return prices

    async def _reset_datepicker(
        self, page: Page, external_id: str
    ) -> bool:
        """Нажимает кнопку «Сбросить» в открытом датепикере.

        Гарантирует сброс ранее выбранных дат заезда/выезда
        перед новым циклом выбора. Сначала ищет кнопку по
        CSS-селектору из HTML-разметки Avito, затем — фоллбэк
        по тексту «Сбросить» среди всех кнопок датепикера.

        Args:
            page: Активная страница Playwright.
            external_id: ID объявления для логирования.

        Returns:
            True если кнопка найдена и нажата.
        """
        # Способ 1: поиск по CSS-селектору кнопки «Сбросить»
        try:
            reset_btn = await page.query_selector(
                DATEPICKER_RESET_BUTTON_SELECTOR
            )
            if reset_btn:
                await reset_btn.click()
                logger.debug(
                    "datepicker_reset_by_css",
                    external_id=external_id,
                )
                return True
        except Exception as e:
            logger.debug(
                "datepicker_reset_css_failed",
                external_id=external_id,
                error=str(e),
            )

        # Способ 2: поиск кнопки по тексту «Сбросить» внутри датепикера
        try:
            buttons = await page.query_selector_all(
                "[data-marker='datepicker'] button"
            )
            for button in buttons:
                try:
                    button_text = (await button.inner_text()).strip()
                    if "сбросить" in button_text.lower():
                        await button.click()
                        logger.debug(
                            "datepicker_reset_by_text",
                            external_id=external_id,
                        )
                        return True
                except Exception:
                    continue
        except Exception as e:
            logger.debug(
                "datepicker_reset_text_search_failed",
                external_id=external_id,
                error=str(e),
            )

        # Способ 3: поиск через JavaScript по содержимому span
        try:
            reset_found: bool = await page.evaluate("""
                () => {
                    const dp = document.querySelector(
                        '[data-marker="datepicker"]'
                    );
                    if (!dp) return false;
                    const buttons = dp.querySelectorAll('button');
                    for (const btn of buttons) {
                        const spans = btn.querySelectorAll('span');
                        for (const span of spans) {
                            if (span.textContent.trim() === 'Сбросить') {
                                btn.click();
                                return true;
                            }
                        }
                    }
                    return false;
                }
            """)
            if reset_found:
                logger.debug(
                    "datepicker_reset_by_js",
                    external_id=external_id,
                )
                return True
        except Exception as e:
            logger.debug(
                "datepicker_reset_js_failed",
                external_id=external_id,
                error=str(e),
            )

        logger.debug(
            "datepicker_reset_button_not_found",
            external_id=external_id,
        )
        return False

    async def _check_nearest_dates_appeared(
        self, page: Page
    ) -> bool:
        """Проверяет, появилась ли карусель ближайших дат.

        После успешного выбора дат заезда и выезда Avito
        отображает карусель <ul role="listbox" data-marker="nearest-dates">
        с вариантами дат и ценами. Её появление подтверждает,
        что JS Avito обработал клики корректно.

        Args:
            page: Активная страница Playwright.

        Returns:
            True если карусель найдена на странице.
        """
        try:
            await page.wait_for_selector(
                NEAREST_DATES_SELECTOR,
                timeout=NEAREST_DATES_TIMEOUT,
            )
            return True
        except Exception:
            return False

    async def _navigate_datepicker_to_month(
        self,
        page: Page,
        target_year: int,
        target_month: int,
    ) -> bool:
        """Листает датепикер вперёд до нужного месяца.

        Datepicker Avito использует 0-indexed месяц в атрибуте
        data-marker (JS Date формат): апрель → 3, январь → 0.

        Args:
            target_year: Год (4 цифры).
            target_month: Месяц (1-12, обычный формат).

        Returns:
            True если нужный месяц стал видимым.
        """
        target_month_0 = target_month - 1

        for _ in range(MAX_MONTH_SWITCHES):
            is_visible: bool = await page.evaluate(
                f"() => !!document.querySelector("
                f"'[data-marker=\"datepicker/calendar"
                f"({target_year}-{target_month_0})\"]')"
            )
            if is_visible:
                return True

            next_btn = await page.query_selector(
                DATEPICKER_NEXT_BUTTON_SELECTOR
            )
            if not next_btn:
                return False

            disabled = await next_btn.get_attribute("disabled")
            if disabled is not None:
                return False

            await next_btn.click()
            await asyncio.sleep(0.4)

        return False

    async def _click_datepicker_day(
        self,
        page: Page,
        target_year: int,
        target_month: int,
        target_day: int,
    ) -> bool:
        """Кликает на доступный день в видимом месяце датепикера.

        Кликает на элемент с role="button" и
        data-marker="datepicker/content" — родительский контейнер
        кликабельной ячейки дня. Именно этот элемент обрабатывает
        события клика в React-приложении Avito.

        Args:
            target_year: Год.
            target_month: Месяц (1-12).
            target_day: День месяца.

        Returns:
            True если клик выполнен успешно.
        """
        target_month_0 = target_month - 1

        el_handle = await page.evaluate_handle(
            f"""() => {{
                const cal = document.querySelector(
                    '[data-marker="datepicker/calendar'
                    + '({target_year}-{target_month_0})"]'
                );
                if (!cal) return null;
                const contentCells = cal.querySelectorAll(
                    '[data-marker="datepicker/content"]'
                );
                for (const cell of contentCells) {{
                    const dayLabel = cell.querySelector(
                        '[data-marker="datepicker-day-available"]'
                    );
                    if (!dayLabel) continue;
                    if (parseInt(dayLabel.textContent.trim()) === {target_day}) {{
                        return cell;
                    }}
                }}
                return null;
            }}"""
        )

        try:
            el = el_handle.as_element()
            if el:
                await el.click()
                return True
        except Exception:
            pass

        return False

    async def _read_item_price(self, page: Page) -> int | None:
        """Читает текущую цену из элемента цены на странице.

        Avito обновляет цену динамически после выбора дат
        заезда и выезда в датепикере.

        Args:
            page: Активная страница Playwright.

        Returns:
            Цена в рублях или None если не найдена.
        """
        try:
            price_el = await page.query_selector(
                "[data-marker='item-view/item-price']"
            )
            if price_el:
                content = await price_el.get_attribute("content")
                if content:
                    price = int(content.strip())
                    if price > 0:
                        return price
        except (ValueError, TypeError, Exception):
            pass
        return None

    async def _read_min_stay_from_datepicker(
        self,
        page: Page,
        default_min_stay: int,
    ) -> int:
        """Читает мин. срок аренды из текста датепикера.

        После клика на дату заезда Avito отображает в датепикере
        текст вида «Бронь минимум от N суток».

        Args:
            page: Активная страница Playwright.
            default_min_stay: Фоллбэк-значение.

        Returns:
            Минимальный срок аренды в сутках.
        """
        try:
            datepicker_el = await page.query_selector(
                DATEPICKER_CONTAINER_SELECTOR
            )
            if not datepicker_el:
                return default_min_stay

            dp_text = (await datepicker_el.inner_text()).strip()

            patterns = [
                r"(?:бронь|бронирование)\s+минимум\s+от\s+(\d+)\s*"
                r"(?:суток|сут\.|ноч[еёий]*|дн[яей]*)",
                r"(?:минимум|мин\.?)\s*(?:от\s*)?(\d+)\s*"
                r"(?:суток|сут\.|ноч[еёий]*|дн[яей]*)",
                r"(?:от|мин[.\s]*срок[а]?)\s*(\d+)\s*"
                r"(?:суток|сут\.|ноч[еёий]*|дн[яей]*)",
                r"(\d+)\s*(?:суток|сут\.)\s*(?:минимум|мин\.?)",
            ]

            for pattern in patterns:
                match = re.search(pattern, dp_text, re.IGNORECASE)
                if match:
                    value = int(match.group(1))
                    if 1 <= value <= 365:
                        return value

        except Exception:
            pass

        return default_min_stay

    # ==================================================================
    # Навигация и обработка блокировок
    # ==================================================================

    async def _navigate_to_listing(
        self,
        page: Page,
        external_id: str,
        full_url: str,
    ) -> bool:
        """Переходит на карточку объявления и проверяет блокировку.

        При обнаружении блокировки сообщает трекеру здоровья
        о бане текущего прокси и пытается дождаться разблокировки
        стандартным retry-механизмом (5 попыток).

        Args:
            page: Активная страница Playwright.
            external_id: ID объявления для логирования.
            full_url: Полный URL карточки.

        Returns:
            True если страница загружена без блокировки.
        """
        delay = random.uniform(
            CARD_NAVIGATE_DELAY_MIN, CARD_NAVIGATE_DELAY_MAX
        )
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
            return False

        # Проверяем блокировку с retry-логикой
        is_blocked = await self._browser_service._check_blocked()
        if is_blocked:
            # Сообщаем трекеру о бане текущего прокси
            self._browser_service.report_ban()

            unblocked = await self._wait_for_listing_unblock(
                page, external_id, full_url
            )
            if not unblocked:
                return False

        # Навигация успешна — сообщаем трекеру
        self._browser_service.report_success()
        return True

    async def _retry_with_proxy_rotation(
        self,
        external_id: str,
        full_url: str,
    ) -> Page | None:
        """Пробует сменить прокси и повторно зайти на карточку.

        После каждой неудачной ротации сообщает трекеру о бане.
        После успеха — сообщает об успехе. Трекер автоматически
        исключит прокси с высоким процентом банов из пула,
        ускоряя ротацию на последующих карточках.

        Выполняет до MAX_PROXY_ROTATIONS_PER_LISTING ротаций прокси.
        После каждой ротации — навигация на карточку + стандартный
        retry при блокировке. Если все ротации исчерпаны — возвращает None.

        Args:
            external_id: ID объявления для логирования.
            full_url: Полный URL карточки.

        Returns:
            Новая Page, на которой карточка загружена, или None.
        """
        if not self._browser_service.has_proxies:
            logger.warning(
                "proxy_rotation_unavailable",
                external_id=external_id,
                reason="нет здоровых прокси в пуле",
            )
            return None

        for rotation in range(1, MAX_PROXY_ROTATIONS_PER_LISTING + 1):
            logger.info(
                "proxy_rotation_for_listing",
                external_id=external_id,
                rotation=rotation,
                max_rotations=MAX_PROXY_ROTATIONS_PER_LISTING,
            )
            print(
                f"  [прокси] Смена прокси для карточки "
                f"{external_id} (попытка {rotation}/"
                f"{MAX_PROXY_ROTATIONS_PER_LISTING})"
            )

            try:
                new_page = await self._browser_service.rotate_proxy()
            except RuntimeError as e:
                logger.error(
                    "proxy_rotation_failed",
                    external_id=external_id,
                    rotation=rotation,
                    error=str(e),
                )
                return None

            navigated = await self._navigate_to_listing(
                new_page, external_id, full_url
            )

            if navigated:
                logger.info(
                    "listing_loaded_after_proxy_rotation",
                    external_id=external_id,
                    rotation=rotation,
                )
                print(
                    f"  [прокси] Карточка {external_id} загружена "
                    f"после смены прокси (попытка {rotation})"
                )
                return new_page

            # Навигация не удалась — _navigate_to_listing уже
            # вызвал report_ban() внутри себя
            logger.warning(
                "listing_still_blocked_after_rotation",
                external_id=external_id,
                rotation=rotation,
            )
            print(
                f"  [прокси] Карточка {external_id} всё ещё "
                f"заблокирована после смены прокси "
                f"(попытка {rotation}/"
                f"{MAX_PROXY_ROTATIONS_PER_LISTING})"
            )

        logger.error(
            "all_proxy_rotations_exhausted_for_listing",
            external_id=external_id,
            max_rotations=MAX_PROXY_ROTATIONS_PER_LISTING,
        )
        return None

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

        # Все retry исчерпаны — бан подтверждён
        self._browser_service.report_ban()

        logger.error(
            "listing_block_not_resolved",
            external_id=external_id,
            max_attempts=MAX_LISTING_UNBLOCK_RETRIES,
            total_wait_seconds=(
                MAX_LISTING_UNBLOCK_RETRIES * LISTING_UNBLOCK_WAIT
            ),
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
        self, page: Page, external_id: str, full_url: str
    ) -> dict:
        """Извлекает календарь занятости из datepicker на странице.

        Если datepicker не подгрузился — ожидает 10 секунд,
        перезагружает страницу и пробует снова (до 3 попыток).

        Args:
            page: Активная страница Playwright.
            external_id: ID объявления для логирования.
            full_url: Полный URL карточки для перезагрузки страницы.

        Returns:
            Словарь с ключами: prices, calendar, minStay.
        """
        result: dict = {
            "prices": [],
            "calendar": [],
            "minStay": 1,
        }

        datepicker_opened = await self._open_datepicker(
            page, external_id
        )

        # Если datepicker не подгрузился — перезагружаем страницу
        # и пробуем снова (до MAX_CALENDAR_RELOAD_RETRIES попыток)
        if not datepicker_opened:
            datepicker_opened = await self._retry_open_datepicker(
                page, external_id, full_url
            )

        if not datepicker_opened:
            logger.warning(
                "datepicker_not_opened_after_all_retries",
                external_id=external_id,
                max_retries=MAX_CALENDAR_RELOAD_RETRIES,
            )
            return result

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
                break

            switched = await self._click_next_month(page, external_id)
            if not switched:
                break

            months_switched += 1
            await asyncio.sleep(random.uniform(0.5, 1.0))

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

        result["minStay"] = await self._extract_min_stay(
            page, external_id
        )

        await self._close_datepicker(page)

        return result

    async def _retry_open_datepicker(
        self,
        page: Page,
        external_id: str,
        full_url: str,
    ) -> bool:
        """Перезагружает страницу и повторно пытается открыть datepicker.

        При обнаружении блокировки после перезагрузки — сообщает
        трекеру о бане текущего прокси.

        Выполняет до MAX_CALENDAR_RELOAD_RETRIES попыток:
        ожидание CALENDAR_RELOAD_WAIT_SECONDS секунд →
        перезагрузка страницы → попытка открыть datepicker.

        Args:
            page: Активная страница Playwright.
            external_id: ID объявления для логирования.
            full_url: Полный URL карточки для перезагрузки.

        Returns:
            True если datepicker удалось открыть после перезагрузки.
        """
        for attempt in range(1, MAX_CALENDAR_RELOAD_RETRIES + 1):
            logger.warning(
                "calendar_not_loaded_retrying",
                external_id=external_id,
                attempt=attempt,
                max_attempts=MAX_CALENDAR_RELOAD_RETRIES,
                wait_seconds=CALENDAR_RELOAD_WAIT_SECONDS,
            )
            print(
                f"  [календарь] Календарь не подгрузился для "
                f"{external_id}. Попытка {attempt}/"
                f"{MAX_CALENDAR_RELOAD_RETRIES}: ожидание "
                f"{CALENDAR_RELOAD_WAIT_SECONDS} сек, "
                f"затем перезагрузка..."
            )

            await asyncio.sleep(CALENDAR_RELOAD_WAIT_SECONDS)

            try:
                await page.reload(
                    wait_until="domcontentloaded",
                    timeout=60000,
                )
                await asyncio.sleep(random.uniform(3.0, 5.0))

                logger.info(
                    "page_reloaded_for_calendar",
                    external_id=external_id,
                    attempt=attempt,
                )
            except Exception as e:
                logger.warning(
                    "page_reload_failed_for_calendar",
                    external_id=external_id,
                    attempt=attempt,
                    error=str(e),
                )
                print(
                    f"  [календарь] Ошибка перезагрузки страницы: {e}"
                )
                continue

            # Проверяем, не заблокирована ли страница после reload
            is_blocked = await self._browser_service._check_blocked()
            if is_blocked:
                # Сообщаем трекеру о бане
                self._browser_service.report_ban()

                logger.warning(
                    "page_blocked_after_reload_for_calendar",
                    external_id=external_id,
                    attempt=attempt,
                )
                print(
                    f"  [календарь] Страница заблокирована после "
                    f"перезагрузки (попытка {attempt}/"
                    f"{MAX_CALENDAR_RELOAD_RETRIES})"
                )
                continue

            # Пробуем открыть datepicker заново
            datepicker_opened = await self._open_datepicker(
                page, external_id
            )
            if datepicker_opened:
                logger.info(
                    "datepicker_opened_after_reload",
                    external_id=external_id,
                    attempt=attempt,
                )
                print(
                    f"  [календарь] Календарь подгрузился после "
                    f"перезагрузки (попытка {attempt})!"
                )
                return True

            logger.warning(
                "datepicker_still_not_loaded_after_reload",
                external_id=external_id,
                attempt=attempt,
            )
            print(
                f"  [календарь] Календарь по-прежнему не найден "
                f"(попытка {attempt}/{MAX_CALENDAR_RELOAD_RETRIES})"
            )

        logger.error(
            "datepicker_not_loaded_all_reloads_exhausted",
            external_id=external_id,
            max_retries=MAX_CALENDAR_RELOAD_RETRIES,
            total_wait_seconds=(
                MAX_CALENDAR_RELOAD_RETRIES
                * CALENDAR_RELOAD_WAIT_SECONDS
            ),
        )
        print(
            f"  [календарь] Не удалось загрузить календарь для "
            f"{external_id} после "
            f"{MAX_CALENDAR_RELOAD_RETRIES} перезагрузок "
            f"({MAX_CALENDAR_RELOAD_RETRIES * CALENDAR_RELOAD_WAIT_SECONDS} сек)"
        )
        return False

    # ==================================================================
    # Datepicker: открытие, парсинг дней, листание, закрытие
    # ==================================================================

    async def _open_datepicker(
        self, page: Page, external_id: str
    ) -> bool:
        """Открывает datepicker кликом на поле ввода даты.

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

        Args:
            page: Активная страница Playwright.
            external_id: ID объявления для логирования.

        Returns:
            Список словарей с ключами year, month, day, disabled.
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
        )

        return sorted_days

    async def _click_next_month(
        self, page: Page, external_id: str
    ) -> bool:
        """Нажимает кнопку переключения на следующий месяц.

        Args:
            page: Активная страница Playwright.
            external_id: ID объявления для логирования.

        Returns:
            True если кнопка найдена и нажата.
        """
        try:
            next_button = await page.query_selector(
                DATEPICKER_NEXT_BUTTON_SELECTOR
            )
            if not next_button:
                return False

            is_disabled = await next_button.get_attribute("disabled")
            if is_disabled is not None:
                return False

            await next_button.click()
            await asyncio.sleep(random.uniform(0.5, 1.0))
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
        """Извлекает минимальный срок аренды.

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

        Args:
            page: Активная страница Playwright.
            external_id: ID объявления для логирования.

        Returns:
            Минимальный срок в сутках или None.
        """
        max_attempts = 5
        day_indices_to_try = [0, 1, 3, 5, 8]

        for attempt_num, day_index in enumerate(
            day_indices_to_try[:max_attempts], start=1
        ):
            datepicker_visible = await page.query_selector(
                DATEPICKER_CONTAINER_SELECTOR
            )
            if not datepicker_visible:
                reopened = await self._open_datepicker(page, external_id)
                if not reopened:
                    return None
                await asyncio.sleep(0.5)

            free_content_cells = await page.query_selector_all(
                "[data-marker='datepicker'] "
                "[data-marker='datepicker/content']:not([data-disabled='true'])"
            )

            # Фильтруем только те, которые содержат доступный день
            clickable_cells: list = []
            for cell in free_content_cells:
                has_available = await cell.query_selector(
                    "[data-marker='datepicker-day-available']"
                )
                if has_available:
                    clickable_cells.append(cell)

            if not clickable_cells:
                return None

            total_free = len(clickable_cells)
            actual_index = min(day_index, total_free - 1)
            cell_el = clickable_cells[actual_index]

            try:
                await cell_el.click()
            except Exception:
                await self._close_datepicker(page)
                await asyncio.sleep(0.5)
                continue

            await asyncio.sleep(1.0)

            datepicker_el = await page.query_selector(
                DATEPICKER_CONTAINER_SELECTOR
            )
            if not datepicker_el:
                await asyncio.sleep(0.5)
                continue

            dp_text = (await datepicker_el.inner_text()).strip()

            dp_match = re.search(
                r"(?:бронь|минимум|мин\.?)\s*(?:от\s*)?(\d+)\s*"
                r"(?:ноч[еёий]*|суток|сут\.|дн[яей]*)",
                dp_text,
                re.IGNORECASE,
            )
            if dp_match:
                min_stay_val = int(dp_match.group(1))
                if 1 <= min_stay_val <= 365:
                    logger.debug(
                        "min_stay_extracted_from_datepicker",
                        external_id=external_id,
                        min_stay=min_stay_val,
                        attempt=attempt_num,
                    )
                    await self._close_datepicker(page)
                    return min_stay_val

            await self._close_datepicker(page)
            await asyncio.sleep(0.5)

        return None

    async def _close_datepicker(self, page: Page) -> None:
        """Закрывает datepicker.

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

    # ==================================================================
    # Извлечение мгновенного бронирования
    # ==================================================================

    async def _extract_instant_book(
        self, page: Page, external_id: str
    ) -> bool:
        """Извлекает флаг мгновенного бронирования.

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
        """Определяет категорию жилья.

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
        """Извлекает рейтинг хоста со страницы.

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
        """Извлекает время последнего обновления объявления.

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
                    minute = (
                        int(time_match.group(2)) if time_match else 0
                    )

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
