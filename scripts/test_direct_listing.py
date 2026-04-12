"""Тестовый скрипт для парсинга одного объявления по прямой ссылке.

Переходит напрямую на страницу конкретного объявления Avito
(без обхода каталога), извлекает все детальные данные карточки
(координаты, календарь, мин. срок, рейтинг, мгновенное бронирование),
затем извлекает реальные цены через датепикер для каждого свободного
дня и экспортирует результат в отдельные тестовую БД и Excel-файл.

Запуск:
    python scripts/test_direct_listing.py
"""

import asyncio
import re
import sys
from datetime import date, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from playwright.async_api import Page

from src.config import (
    BrowserSettings,
    ExportSettings,
    ProxySettings,
    get_logger,
    setup_logging,
    set_trace_id,
)
from src.repositories import SQLiteListingRepository
from src.services import BrowserService, ExportService, ListingService

# ============================================================
# Настройки тестового скрипта
# ============================================================

# Прямая ссылка на объявление Avito (без дублирования)
DIRECT_LISTING_URL: str = (
    "https://www.avito.ru/sankt-peterburg/kvartiry/"
    "kvartira-studiya_28_m_1_krovat_2171665897"
)

# Пути к тестовым файлам (отдельно от основных)
TEST_DB_PATH: str = "data/test_direct_listing.db"
TEST_EXPORT_PATH: str = "data/test_direct_listing.xlsx"

# Таймаут ожидания датепикера после клика (мс)
DATEPICKER_OPEN_TIMEOUT: int = 5000

# Максимальное количество переключений месяцев при навигации вперёд
MAX_MONTH_SWITCHES: int = 4

# CSS-селектор кнопки «Сбросить» в датепикере (основной)
DATEPICKER_RESET_BUTTON_SELECTOR: str = (
    "button._8761af61d40d8964.f6eebfeb30fe503c._793efba06309a0ff"
)

logger = get_logger("test_direct_listing")


# ============================================================
# Вспомогательные функции: ID из URL
# ============================================================


def _extract_avito_id_from_url(url: str) -> str:
    """Извлекает числовой ID объявления из URL Avito.

    Avito URL заканчивается на «_<числовой_id>», например:
    «kvartira-studiya_28_m_1_krovat_2171665897» → «2171665897».

    Args:
        url: Полный или относительный URL объявления Avito.

    Returns:
        Строка external_id в формате «av_<числовой_id>».

    Raises:
        ValueError: Если не удалось извлечь ID из URL.
    """
    match = re.search(r"_(\d{5,15})(?:\?|$)", url)
    if match:
        return f"av_{match.group(1)}"

    # Фоллбэк: ищем любую длинную последовательность цифр
    match_fallback = re.search(r"(\d{7,15})", url)
    if match_fallback:
        return f"av_{match_fallback.group(1)}"

    raise ValueError(
        f"Не удалось извлечь ID объявления из URL: {url}"
    )


def _extract_title_from_url(url: str) -> str:
    """Извлекает приблизительное название из URL.

    Берёт последний сегмент пути URL и преобразует
    подчёркивания и дефисы в пробелы.

    Args:
        url: URL объявления.

    Returns:
        Человекочитаемое название (приблизительное).
    """
    from urllib.parse import urlparse

    path = urlparse(url).path.rstrip("/")
    last_segment = path.split("/")[-1] if "/" in path else path

    # Убираем числовой ID в конце
    cleaned = re.sub(r"_\d{5,15}$", "", last_segment)
    # Заменяем разделители на пробелы
    cleaned = cleaned.replace("_", " ").replace("-", " ")

    return cleaned.strip().capitalize() or "Объявление Avito"


# ============================================================
# Вспомогательные функции: работа с датепикером
# ============================================================


async def _open_datepicker(page: Page) -> bool:
    """Открывает датепикер кликом на поле ввода даты.

    Если датепикер уже открыт — возвращает True без клика.

    Returns:
        True если датепикер успешно открыт.
    """
    existing = await page.query_selector("[data-marker='datepicker']")
    if existing:
        return True

    input_selectors = [
        "input[placeholder*='Заезд']",
        "input[placeholder*='заезд']",
        "[data-marker='datepicker'] input",
        "input[placeholder*='дата']",
        "input[placeholder*='Дата']",
        "[data-marker*='booking'] input",
    ]

    for sel in input_selectors:
        el = await page.query_selector(sel)
        if el:
            await el.click()
            try:
                await page.wait_for_selector(
                    "[data-marker='datepicker']",
                    timeout=DATEPICKER_OPEN_TIMEOUT,
                )
                return True
            except Exception:
                continue

    return False


async def _reset_datepicker(page: Page) -> bool:
    """Нажимает кнопку «Сбросить» в открытом датепикере.

    Гарантирует сброс ранее выбранных дат заезда/выезда
    перед новым циклом выбора. Сначала ищет кнопку по
    CSS-селектору, затем — фоллбэк по тексту, затем — через JS.

    Args:
        page: Активная страница Playwright.

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
            return True
    except Exception:
        pass

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
                    return True
            except Exception:
                continue
    except Exception:
        pass

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
            return True
    except Exception:
        pass

    return False


async def _close_datepicker(page: Page) -> None:
    """Сбрасывает выбор и закрывает датепикер.

    Пробует последовательно: кнопка «Сбросить» → Escape → клик вне.
    """
    try:
        buttons = await page.query_selector_all(
            "[data-marker='datepicker'] button"
        )
        for btn in buttons:
            try:
                text = (await btn.inner_text()).strip().lower()
                if "сброс" in text or "очист" in text:
                    await btn.click()
                    await asyncio.sleep(0.4)
                    return
            except Exception:
                continue
    except Exception:
        pass

    try:
        await page.keyboard.press("Escape")
        await asyncio.sleep(0.3)
    except Exception:
        pass

    try:
        await page.click("body", position={"x": 10, "y": 10})
        await asyncio.sleep(0.3)
    except Exception:
        pass


async def _navigate_to_month(
    page: Page, target_year: int, target_month: int
) -> bool:
    """Листает датепикер вперёд до нужного месяца.

    Датепикер Avito использует 0-indexed месяц в атрибуте
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
            "[data-marker='datepicker/next-button']"
        )
        if not next_btn:
            return False

        disabled = await next_btn.get_attribute("disabled")
        if disabled is not None:
            return False

        await next_btn.click()
        await asyncio.sleep(0.4)

    return False


async def _click_day(
    page: Page, target_year: int, target_month: int, target_day: int
) -> bool:
    """Кликает на доступный день в видимом месяце датепикера.

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

            const dayContents = cal.querySelectorAll(
                '[data-marker="datepicker/content"]'
            );
            for (const dayEl of dayContents) {{
                const inner = dayEl.querySelector(
                    '[data-marker="datepicker-day-available"]'
                );
                if (!inner) continue;
                if (parseInt(inner.textContent.trim()) === {target_day}) {{
                    return inner;
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


async def _read_min_stay_from_datepicker(
    page: Page, default_min_stay: int
) -> int:
    """Читает минимальный срок аренды из текста датепикера.

    После клика на дату заезда Avito отображает в датепикере
    текст вида «Бронь минимум от N суток» (или «ночей», «дней»).
    Функция извлекает число N из этого текста.

    Args:
        page: Активная страница Playwright.
        default_min_stay: Фоллбэк-значение мин. срока.

    Returns:
        Минимальный срок аренды в сутках.
    """
    try:
        datepicker_el = await page.query_selector(
            "[data-marker='datepicker']"
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
            r"минимальн\w*\s*(?:срок\w*)?\s*(\d+)\s*"
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


async def _read_item_price(page: Page) -> int | None:
    """Читает текущую цену из span[data-marker='item-view/item-price'].

    Returns:
        Цена в рублях или None если элемент не найден.
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


async def _read_base_price(page: Page) -> int:
    """Читает базовую цену со страницы карточки до работы с датепикером.

    Пробует несколько источников: атрибут content элемента цены,
    текст элемента цены, __initialData__.

    Args:
        page: Активная страница на карточке объявления.

    Returns:
        Базовая цена в рублях (0 если не удалось прочитать).
    """
    # Способ 1: атрибут content
    price = await _read_item_price(page)
    if price is not None:
        return price

    # Способ 2: текст элемента цены
    try:
        price_el = await page.query_selector(
            "[data-marker='item-view/item-price']"
        )
        if price_el:
            text = (await price_el.inner_text()).strip()
            digits = re.sub(r"[^\d]", "", text)
            if digits:
                return int(digits)
    except Exception:
        pass

    # Способ 3: __initialData__
    try:
        js_price = await page.evaluate("""
            () => {
                try {
                    const data = window.__initialData__;
                    if (data) {
                        const text = JSON.stringify(data);
                        const match = text.match(/"price":\\s*(\\d+)/);
                        if (match) return parseInt(match[1]);
                    }
                } catch (e) {}
                return 0;
            }
        """)
        if js_price and js_price > 0:
            return js_price
    except Exception:
        pass

    return 0


# ============================================================
# Извлечение реальных цен через датепикер
# ============================================================


async def extract_prices_for_free_days(
    page: Page,
    calendar_60_days: list[int],
    default_min_stay: int,
    base_price: int,
) -> list[int]:
    """Извлекает реальные цены для каждого свободного дня через датепикер.

    Для каждого свободного дня (calendar_60_days[i] == 0):
    1. Открывает датепикер.
    2. Нажимает кнопку «Сбросить» для гарантированного
       сброса ранее выбранных дат.
    3. Навигирует до месяца даты заезда.
    4. Кликает на дату заезда (check-in).
    5. Читает минимальный срок из текста датепикера.
    6. Вычисляет дату выезда = заезд + мин. срок из датепикера.
    7. Навигирует до месяца выезда, кликает дату выезда.
    8. Читает цену из span[data-marker='item-view/item-price'].
    9. Сбрасывает датепикер перед следующей итерацией.

    Занятые дни получают цену 0.

    Args:
        page: Активная страница Playwright.
        calendar_60_days: Массив занятости 60 дней (0 — свободен,
                          1 — занят).
        default_min_stay: Фоллбэк мин. срока аренды.
        base_price: Цена-фоллбэк из карточки при ошибке чтения.

    Returns:
        Массив цен на 60 дней: 0 для занятых, реальная цена для
        свободных (или base_price при ошибке).
    """
    today = date.today()
    prices: list[int] = [0] * 60

    free_days = [i for i in range(60) if calendar_60_days[i] == 0]
    print(
        f"\n[цены] Свободных дней: {len(free_days)} из 60 "
        f"→ начинаю обход датепикера"
    )
    print(
        f"[цены] Фоллбэк мин. срока (из карточки): "
        f"{default_min_stay} сут."
    )

    for day_idx in free_days:
        checkin = today + timedelta(days=day_idx)

        print(
            f"  [день {day_idx:02d}] заезд {checkin} ...",
            end=" ",
        )

        # --- Шаг 1: закрываем датепикер если открыт ---
        existing_dp = await page.query_selector(
            "[data-marker='datepicker']"
        )
        if existing_dp:
            await _close_datepicker(page)
            await asyncio.sleep(0.3)

        # --- Шаг 2: открыть датепикер ---
        opened = await _open_datepicker(page)
        if not opened:
            print("датепикер не открылся → фоллбэк")
            prices[day_idx] = base_price
            continue

        await asyncio.sleep(0.3)

        # --- Шаг 2.5: сброс датепикера кнопкой «Сбросить» ---
        await _reset_datepicker(page)
        await asyncio.sleep(0.3)

        # --- Шаг 3: навигация до месяца заезда ---
        month_found = await _navigate_to_month(
            page, checkin.year, checkin.month
        )
        if not month_found:
            print(
                f"месяц {checkin.month}/{checkin.year} не найден "
                f"→ фоллбэк"
            )
            await _close_datepicker(page)
            prices[day_idx] = base_price
            continue

        # --- Шаг 4: клик на дату заезда ---
        clicked_in = await _click_day(
            page, checkin.year, checkin.month, checkin.day
        )
        if not clicked_in:
            print(
                f"дата заезда {checkin.day} не кликнута → фоллбэк"
            )
            await _close_datepicker(page)
            prices[day_idx] = base_price
            continue

        await asyncio.sleep(0.5)

        # --- Шаг 5: читаем мин. срок из текста датепикера ---
        actual_min_stay = await _read_min_stay_from_datepicker(
            page, default_min_stay
        )
        checkout = checkin + timedelta(days=actual_min_stay)

        print(
            f"мин.срок={actual_min_stay} сут. → выезд {checkout}",
            end=" → ",
        )

        # --- Шаг 6: навигация до месяца выезда и клик ---
        if actual_min_stay >= 1:
            checkout_month_found = await _navigate_to_month(
                page, checkout.year, checkout.month
            )
            if checkout_month_found:
                clicked_out = await _click_day(
                    page, checkout.year, checkout.month, checkout.day
                )
                if not clicked_out:
                    print(
                        f"дата выезда {checkout} не кликнута",
                        end=" → ",
                    )
                await asyncio.sleep(0.5)
            else:
                print(
                    f"месяц выезда {checkout.month}/{checkout.year} "
                    f"не найден",
                    end=" → ",
                )

        # --- Шаг 7: читаем цену ---
        price = await _read_item_price(page)
        if price is not None:
            prices[day_idx] = price
            print(f"цена: {price} руб.")
        else:
            prices[day_idx] = base_price
            print(f"цена не найдена → фоллбэк {base_price} руб.")

        # --- Шаг 8: сброс датепикера ---
        await _close_datepicker(page)
        await asyncio.sleep(0.3)

    filled = sum(1 for i in free_days if prices[i] > 0)
    print(
        f"\n[цены] Готово: {filled}/{len(free_days)} "
        f"свободных дней получили цену"
    )

    return prices


# ============================================================
# Основной конвейер
# ============================================================


async def run_direct_pipeline() -> None:
    """Запускает конвейер парсинга одного объявления по прямой ссылке.

    Этапы:
    1. Запускает браузер и переходит на страницу объявления.
    2. Извлекает базовую цену со страницы.
    3. Извлекает все детальные данные через ListingService.
    4. Извлекает реальные цены через датепикер.
    5. Сохраняет в тестовую БД SQLite.
    6. Экспортирует в тестовый Excel-файл.
    """
    # --- Извлекаем ID и название из URL ---
    try:
        external_id = _extract_avito_id_from_url(DIRECT_LISTING_URL)
    except ValueError as e:
        print(f"\n[ОШИБКА] {e}")
        return

    approx_title = _extract_title_from_url(DIRECT_LISTING_URL)

    print(f"\n{'=' * 60}")
    print("Целевое объявление:")
    print(f"  URL:         {DIRECT_LISTING_URL}")
    print(f"  External ID: {external_id}")
    print(f"  Название:    {approx_title} (приблизительно из URL)")
    print(f"{'=' * 60}\n")

    # --- Подготовка инфраструктуры ---
    browser_settings = BrowserSettings(
        headless=False,
        navigation_timeout=90000,
        page_wait_time=30000,
    )

    # Тестовый скрипт работает без прокси
    proxy_settings = ProxySettings(
        proxy_file_path="",
        rotate_every_n=0,
    )

    repository = SQLiteListingRepository(db_path=TEST_DB_PATH)
    repository.initialize()

    browser_service = BrowserService(
        settings=browser_settings,
        proxy_settings=proxy_settings,
    )

    try:
        # === ЭТАП 1: Запуск браузера и навигация ===
        logger.info(
            "direct_test_started",
            url=DIRECT_LISTING_URL,
            external_id=external_id,
        )
        print("[этап 1] Запуск браузера и навигация на объявление...")

        page = await browser_service.launch()

        success = await browser_service.navigate(DIRECT_LISTING_URL)
        if not success:
            logger.error(
                "direct_test_navigation_failed",
                url=DIRECT_LISTING_URL,
            )
            print("\n[ОШИБКА] Не удалось загрузить страницу объявления.")
            print("Возможные причины:")
            print("  - Avito заблокировал доступ (CAPTCHA)")
            print("  - Объявление удалено или недоступно")
            print("  - Проблемы с интернет-соединением")
            return

        await browser_service.simulate_human_behavior()
        print("[этап 1] Страница объявления загружена успешно.\n")

        # === ЭТАП 2: Чтение базовой цены со страницы ===
        print("[этап 2] Чтение базовой цены со страницы...")

        base_price = await _read_base_price(page)
        if base_price > 0:
            print(f"  Базовая цена: {base_price} руб./сут.")
        else:
            base_price = 1
            print(
                "  Базовая цена не найдена → используется заглушка 1 руб."
            )

        # Читаем реальное название со страницы (если доступно)
        real_title = approx_title
        try:
            title_el = await page.query_selector(
                "[data-marker='item-view/title-info'] h1,"
                "h1[data-marker='item-view/title'],"
                "h1[itemprop='name'],"
                "h1"
            )
            if title_el:
                real_title = (await title_el.inner_text()).strip()
                print(f"  Название:     {real_title}")
        except Exception:
            print(f"  Название:     {approx_title} (из URL)")

        # === ЭТАП 3: Детальный парсинг карточки через ListingService ===
        print("\n[этап 3] Парсинг карточки объявления (ListingService)...")

        listing_service = ListingService(browser_service=browser_service)

        # Извлекаем относительный URL для ListingService
        relative_url = DIRECT_LISTING_URL.replace(
            "https://www.avito.ru", ""
        )
        # Убираем query-параметры
        if "?" in relative_url:
            relative_url = relative_url.split("?")[0]

        listing = await listing_service.parse_listing(
            page=page,
            external_id=external_id,
            url=relative_url,
            title=real_title,
            base_price=base_price,
            is_instant_book=False,
            catalog_host_rating=0.0,
        )

        if listing is None:
            logger.error(
                "direct_test_listing_parse_failed",
                external_id=external_id,
            )
            print("\n[ОШИБКА] Не удалось спарсить карточку объявления.")
            print("Возможные причины:")
            print("  - Avito заблокировала доступ к карточке")
            print("  - Страница объявления не загрузилась")
            print("  - Изменилась структура карточки объявления")
            return

        print(f"\n  Результаты парсинга карточки:")
        print(f"    External ID:      {listing.external_id}")
        print(f"    Название:         {listing.title}")
        print(f"    Категория жилья:  {listing.room_category.value}")
        print(
            f"    Координаты:       "
            f"{listing.latitude}, {listing.longitude}"
        )
        print(f"    Мин. срок:        {listing.min_stay} сут.")
        print(f"    Занятость:        {listing.occupancy_rate:.1%}")
        print(
            f"    Мгновенная бронь: "
            f"{'Да' if listing.is_instant_book else 'Нет'}"
        )
        print(f"    Рейтинг хоста:    {listing.host_rating}")
        print(
            f"    Последнее обновление: "
            f"{listing.last_host_update or 'Не найдено'}"
        )
        print(f"    Цены (фоллбэк, 7): {listing.price_60_days[:7]}")
        print(f"    Календарь (14):    {listing.calendar_60_days[:14]}")

        # === ЭТАП 4: Возврат на страницу объявления для датепикера ===
        #
        # ListingService.parse_listing() делает page.goto() внутри,
        # поэтому мы уже на странице карточки. Но на всякий случай
        # убеждаемся, что находимся на нужной странице.
        print(
            f"\n[этап 4] Извлечение реальных цен через датепикер..."
        )
        print(
            f"  Фоллбэк мин. срока (из карточки): "
            f"{listing.min_stay} сут."
        )
        print(
            "  Для каждой даты мин. срок будет прочитан "
            "из датепикера индивидуально"
        )

        # Проверяем, что мы на странице карточки
        current_url = page.url
        if external_id.replace("av_", "") not in current_url:
            print(
                "  Переход обратно на страницу объявления..."
            )
            await page.goto(
                DIRECT_LISTING_URL,
                wait_until="domcontentloaded",
                timeout=60000,
            )
            await asyncio.sleep(3.0)

        real_prices = await extract_prices_for_free_days(
            page=page,
            calendar_60_days=listing.calendar_60_days,
            default_min_stay=listing.min_stay,
            base_price=base_price,
        )

        # Обновляем цены в объекте listing
        listing.price_60_days = real_prices

        logger.info(
            "direct_test_prices_extracted",
            external_id=listing.external_id,
            prices_filled=sum(1 for p in real_prices if p > 0),
            avg_price=round(listing.average_price),
        )

        print(f"\n  Цены обновлены:")
        print(
            f"    Средняя цена:    "
            f"{round(listing.average_price)} руб./сут."
        )
        print(f"    Цены (первые 14): {listing.price_60_days[:14]}")

        # === ЭТАП 5: Сохранение в тестовую БД ===
        print(f"\n[этап 5] Сохранение в тестовую БД...")

        repository.save_listing(listing)

        db_count = repository.get_listings_count()
        logger.info(
            "direct_test_saved_to_db",
            external_id=listing.external_id,
            total_in_db=db_count,
        )
        print(
            f"  Сохранено: {TEST_DB_PATH} (записей: {db_count})"
        )

        # === ЭТАП 6: Экспорт в тестовый Excel ===
        print(f"\n[этап 6] Экспорт в Excel...")

        test_export_settings = ExportSettings(
            export_path=TEST_EXPORT_PATH,
        )
        export_service = ExportService(
            repository=repository,
            settings=test_export_settings,
        )
        export_path = export_service.export()

        if export_path:
            logger.info(
                "direct_test_exported",
                file_path=export_path,
            )
            print(f"  Excel-файл создан: {export_path}")
        else:
            print(
                "  [ПРЕДУПРЕЖДЕНИЕ] Excel-файл не создан — нет данных."
            )

        # === Итоговый отчёт ===
        print(f"\n{'=' * 60}")
        print("Тестовый прогон завершён успешно!")
        print(f"  Объявление: {listing.title[:60]}")
        print(f"  External ID: {listing.external_id}")
        print(
            f"  Средняя цена: "
            f"{round(listing.average_price)} руб./сут."
        )
        print(f"  Занятость: {listing.occupancy_rate:.1%}")
        print(f"  Мин. срок: {listing.min_stay} сут.")
        print(f"  БД:    {Path(TEST_DB_PATH).resolve()}")
        if export_path:
            print(f"  Excel: {Path(export_path).resolve()}")
        print(f"{'=' * 60}")

    finally:
        await browser_service.close()
        repository.close()
        logger.info("direct_test_resources_closed")


# ============================================================
# Точка входа
# ============================================================


def main() -> None:
    """Главная функция тестового скрипта.

    Настраивает логирование и запускает конвейер парсинга
    одного объявления по прямой ссылке.
    """
    print(
        "\n=== Avito Parser — тестовый прогон "
        "(прямая ссылка на объявление) ===\n"
    )

    setup_logging(level="DEBUG", log_file_path="")

    trace_id = set_trace_id()

    logger.info(
        "direct_test_application_started",
        trace_id=trace_id,
        url=DIRECT_LISTING_URL,
        test_db=TEST_DB_PATH,
        test_export=TEST_EXPORT_PATH,
    )

    try:
        asyncio.run(run_direct_pipeline())
    except KeyboardInterrupt:
        logger.info("direct_test_interrupted")
        print("\nСкрипт остановлен пользователем (Ctrl+C).")
    except Exception as e:
        logger.critical(
            "direct_test_fatal_error",
            exc_info=True,
            error=str(e),
            error_type=type(e).__name__,
        )
        print(f"\nКритическая ошибка: {e}")
        sys.exit(1)

    logger.info("direct_test_application_finished", trace_id=trace_id)


if __name__ == "__main__":
    main()
