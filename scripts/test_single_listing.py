"""Тестовый скрипт для парсинга одного объявления.

Прогоняет полный pipeline проекта (каталог → карточка → БД → Excel),
но обрабатывает только первое объявление из каталога.
Дополнительно извлекает реальные цены на свободные дни через
датепикер (заезд + выезд → span[data-marker="item-view/item-price"]).
Использует отдельную тестовую БД и отдельный Excel-файл,
чтобы не затрагивать основные данные.

Запуск:
python scripts/test_single_listing.py
"""

import asyncio
import sys
from datetime import date, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from playwright.async_api import Page

from src.config import (
    ConfigValidationError,
    ExportSettings,
    Settings,
    get_logger,
    load_settings,
    set_trace_id,
    setup_logging,
)
from src.repositories import SQLiteListingRepository
from src.services import (
    BrowserService,
    ExportService,
    ListingService,
    ScraperService,
)

# Пути к тестовым файлам (отдельно от основных)
TEST_DB_PATH = "data/test_single_listing.db"
TEST_EXPORT_PATH = "data/test_single_listing.xlsx"

# Таймаут ожидания датепикера после клика (мс)
DATEPICKER_OPEN_TIMEOUT: int = 5000
# Максимальное количество переключений месяцев при навигации вперёд
MAX_MONTH_SWITCHES: int = 4

logger = get_logger("test_single_listing")


# ==================================================================
# Вспомогательные функции: работа с датепикером и ценой
# ==================================================================


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
    target_month_0 = target_month - 1  # переводим в JS Date (0-indexed)

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

    Ищет ячейку с data-marker="datepicker-day-available" внутри
    нужного календарного блока, совпадающую по числу.

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


async def _read_item_price(page: Page) -> int | None:
    """Читает текущую цену из span[data-marker='item-view/item-price'].

    Берёт значение атрибута content (числовое, без форматирования).
    Пример: <span content="4000" data-marker="item-view/item-price">

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


# ==================================================================
# Основная функция извлечения цен
# ==================================================================


async def extract_prices_for_free_days(
    page: Page,
    calendar_60_days: list[int],
    min_stay: int,
    base_price: int,
) -> list[int]:
    """Извлекает реальные цены для каждого свободного дня через датепикер.

    Для каждого свободного дня (calendar_60_days[i] == 0):
    1. Открывает датепикер.
    2. Навигирует до месяца даты заезда.
    3. Кликает на дату заезда (check-in).
    4. Если min_stay > 1: навигирует до месяца выезда, кликает выезд.
    5. Читает цену из span[data-marker='item-view/item-price'] → content.
    6. Сбрасывает датепикер перед следующей итерацией.
    Занятые дни получают цену 0.

    Args:
        page: Активная страница Playwright (должна быть на странице
              карточки объявления).
        calendar_60_days: Массив занятости 60 дней (0 — свободен,
                          1 — занят).
        min_stay: Минимальный срок аренды в сутках.
        base_price: Цена-фоллбэк при ошибке чтения.

    Returns:
        Массив цен на 60 дней: 0 для занятых, реальная цена для
        свободных (или base_price при ошибке).
    """
    today = date.today()
    prices = [0] * 60

    free_days = [i for i in range(60) if calendar_60_days[i] == 0]
    print(
        f"\n[цены] Свободных дней: {len(free_days)} из 60 "
        f"→ начинаю обход датепикера"
    )

    for day_idx in free_days:
        checkin = today + timedelta(days=day_idx)
        checkout = checkin + timedelta(days=min_stay)

        print(
            f"  [день {day_idx:02d}] заезд {checkin} "
            f"→ выезд {checkout} (мин. {min_stay} сут.) ...",
            end=" ",
        )

        # --- Шаг 1: открыть датепикер ---
        opened = await _open_datepicker(page)
        if not opened:
            print("датепикер не открылся → пропуск")
            prices[day_idx] = base_price
            continue

        await asyncio.sleep(0.3)

        # --- Шаг 2: навигация до месяца заезда ---
        month_found = await _navigate_to_month(
            page, checkin.year, checkin.month
        )
        if not month_found:
            print(f"месяц {checkin.month}/{checkin.year} не найден → пропуск")
            await _close_datepicker(page)
            prices[day_idx] = base_price
            continue

        # --- Шаг 3: клик на дату заезда ---
        clicked_in = await _click_day(
            page, checkin.year, checkin.month, checkin.day
        )
        if not clicked_in:
            print(f"дата заезда {checkin.day} не кликнута → пропуск")
            await _close_datepicker(page)
            prices[day_idx] = base_price
            continue

        await asyncio.sleep(0.5)

        # --- Шаг 4: клик на дату выезда (если min_stay > 1) ---
        if min_stay > 1:
            checkout_month_found = await _navigate_to_month(
                page, checkout.year, checkout.month
            )
            if checkout_month_found:
                await _click_day(
                    page, checkout.year, checkout.month, checkout.day
                )
                await asyncio.sleep(0.5)

        # --- Шаг 5: читаем цену ---
        price = await _read_item_price(page)
        if price is not None:
            prices[day_idx] = price
            print(f"цена: {price} руб.")
        else:
            prices[day_idx] = base_price
            print(f"цена не найдена → фоллбэк {base_price} руб.")

        # --- Шаг 6: сброс датепикера ---
        await _close_datepicker(page)
        await asyncio.sleep(0.3)

    filled = sum(1 for i in free_days if prices[i] > 0)
    print(
        f"\n[цены] Готово: {filled}/{len(free_days)} "
        f"свободных дней получили цену"
    )

    return prices


# ==================================================================
# Основной конвейер
# ==================================================================


async def run_test_pipeline(settings: Settings) -> None:
    """Запускает тестовый конвейер: парсинг одного объявления.

    Этапы:
    1. Открывает браузер и переходит на каталог Avito.
    2. Парсит карточки на первой странице каталога.
    3. Берёт только первое объявление.
    4. Заходит в карточку и извлекает базовые данные (координаты,
       календарь, мин. срок, рейтинг).
    5. Извлекает реальные цены через датепикер.
    6. Обновляет listing.price_60_days реальными ценами.
    7. Сохраняет в тестовую БД SQLite.
    8. Экспортирует в тестовый Excel-файл.

    Args:
        settings: Валидированные настройки приложения.
    """
    repository = SQLiteListingRepository(db_path=TEST_DB_PATH)
    repository.initialize()

    browser_service = BrowserService(settings=settings.browser)

    try:
        # === ЭТАП 1: Запуск браузера и навигация ===
        logger.info(
            "test_stage_started",
            stage="browser_launch",
            url=settings.scraper.category_url,
        )

        page = await browser_service.launch()

        success = await browser_service.navigate(
            settings.scraper.category_url
        )
        if not success:
            logger.error(
                "test_navigation_failed",
                url=settings.scraper.category_url,
            )
            print("\n[ОШИБКА] Не удалось загрузить страницу каталога Avito.")
            print("Возможные причины:")
            print("  - Avito заблокировал доступ (CAPTCHA)")
            print("  - Некорректный URL в AVITO_CATEGORY_URL")
            print("  - Проблемы с интернет-соединением")
            return

        await browser_service.simulate_human_behavior()
        logger.info("test_stage_completed", stage="browser_launch")

        # === ЭТАП 2: Парсинг первой страницы каталога ===
        logger.info("test_stage_started", stage="catalog_parse")

        listing_service = ListingService(browser_service=browser_service)
        scraper_service = ScraperService(
            browser_service=browser_service,
            listing_service=listing_service,
            repository=repository,
            settings=settings.scraper,
        )

        catalog_items = await scraper_service._parse_current_page(page)

        if not catalog_items:
            logger.error("test_no_catalog_items")
            print(
                "\n[ОШИБКА] На странице каталога не найдено "
                "ни одного объявления."
            )
            print("Возможные причины:")
            print("  - Avito показала CAPTCHA или заглушку")
            print("  - Изменились CSS-селекторы каталога")
            print("  - Страница не успела загрузиться")
            return

        first_item = catalog_items[0]

        logger.info(
            "test_first_item_selected",
            avito_id=first_item.avito_id,
            title=first_item.title[:80],
            price=first_item.price,
            url=first_item.url[:100],
            is_instant_book=first_item.is_instant_book,
            host_rating=first_item.host_rating,
        )

        print(f"\n{'=' * 60}")
        print(f"Найдено объявлений на странице: {len(catalog_items)}")
        print("Выбрано первое объявление:")
        print(f"  ID:              {first_item.avito_id}")
        print(f"  Название:        {first_item.title}")
        print(f"  Цена (каталог):  {first_item.price} руб./сут.")
        print(
            f"  Мгновенная бронь:"
            f" {'Да' if first_item.is_instant_book else 'Нет'}"
        )
        print(f"  Рейтинг:         {first_item.host_rating}")
        print(f"  Ссылка:          https://www.avito.ru{first_item.url}")
        print(f"{'=' * 60}\n")

        # === ЭТАП 3: Детальный парсинг карточки ===
        logger.info(
            "test_stage_started",
            stage="listing_detail_parse",
            external_id=first_item.external_id,
        )

        listing = await listing_service.parse_listing(
            page=page,
            external_id=first_item.external_id,
            url=first_item.url,
            title=first_item.title,
            base_price=first_item.price,
            is_instant_book=first_item.is_instant_book,
            catalog_host_rating=first_item.host_rating,
        )

        if listing is None:
            logger.error(
                "test_listing_parse_failed",
                external_id=first_item.external_id,
            )
            print("[ОШИБКА] Не удалось спарсить карточку объявления.")
            print("Возможные причины:")
            print("  - Avito заблокировала доступ к карточке")
            print("  - Страница объявления не загрузилась")
            print("  - Изменилась структура карточки объявления")
            return

        logger.info(
            "test_stage_completed",
            stage="listing_detail_parse",
            external_id=listing.external_id,
            room_category=listing.room_category.value,
            latitude=listing.latitude,
            longitude=listing.longitude,
            avg_price=round(listing.average_price),
            occupancy=f"{listing.occupancy_rate:.0%}",
            min_stay=listing.min_stay,
            is_instant_book=listing.is_instant_book,
            host_rating=listing.host_rating,
        )

        print("Результаты парсинга карточки:")
        print(f"  External ID:      {listing.external_id}")
        print(f"  Категория жилья:  {listing.room_category.value}")
        print(f"  Координаты:       {listing.latitude}, {listing.longitude}")
        print(f"  Мин. срок:        {listing.min_stay} сут.")
        print(f"  Занятость:        {listing.occupancy_rate:.1%}")
        print(
            f"  Мгновенная бронь: "
            f"{'Да' if listing.is_instant_book else 'Нет'}"
        )
        print(f"  Рейтинг хоста:    {listing.host_rating}")
        print(
            f"  Последнее обновление: "
            f"{listing.last_host_update or 'Не найдено'}"
        )
        print(f"  Цены (фоллбэк, 7): {listing.price_60_days[:7]}")
        print(f"  Календарь (14):    {listing.calendar_60_days[:14]}")

        # === ЭТАП 4: Реальные цены через датепикер ===
        logger.info(
            "test_stage_started",
            stage="price_extraction",
            external_id=listing.external_id,
            min_stay=listing.min_stay,
            free_days=listing.calendar_60_days.count(0),
        )

        print(f"\n{'=' * 60}")
        print("Этап: извлечение реальных цен через датепикер")
        print(f"  Мин. срок аренды: {listing.min_stay} сут.")

        real_prices = await extract_prices_for_free_days(
            page=page,
            calendar_60_days=listing.calendar_60_days,
            min_stay=listing.min_stay,
            base_price=first_item.price,
        )

        # Обновляем цены в объекте listing
        listing.price_60_days = real_prices

        logger.info(
            "test_stage_completed",
            stage="price_extraction",
            external_id=listing.external_id,
            prices_filled=sum(1 for p in real_prices if p > 0),
            avg_price=round(listing.average_price),
        )

        print(f"\nЦены обновлены в listing:")
        print(f"  Средняя цена:    {round(listing.average_price)} руб./сут.")
        print(f"  Цены (первые 7): {listing.price_60_days[:7]}")
        print(f"{'=' * 60}\n")

        # === ЭТАП 5: Сохранение в тестовую БД ===
        logger.info("test_stage_started", stage="database_save")

        repository.save_listing(listing)

        db_count = repository.get_listings_count()
        logger.info(
            "test_stage_completed",
            stage="database_save",
            total_in_db=db_count,
        )
        print(
            f"Сохранено в тестовую БД: {TEST_DB_PATH} "
            f"(записей: {db_count})"
        )

        # === ЭТАП 6: Экспорт в тестовый Excel ===
        logger.info("test_stage_started", stage="excel_export")

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
                "test_stage_completed",
                stage="excel_export",
                file_path=export_path,
            )
            print(f"Excel-файл создан: {export_path}")
        else:
            logger.warning("test_export_no_data")
            print("[ПРЕДУПРЕЖДЕНИЕ] Excel-файл не создан — нет данных.")

        print(f"\n{'=' * 60}")
        print("Тестовый прогон завершён успешно!")
        print(f"  БД:    {Path(TEST_DB_PATH).resolve()}")
        if export_path:
            print(f"  Excel: {export_path}")
        print(f"{'=' * 60}")

    finally:
        await browser_service.close()
        repository.close()
        logger.info("test_all_resources_closed")


def main() -> None:
    """Главная функция тестового скрипта.

    Загружает конфигурацию из .env, настраивает логирование
    и запускает тестовый конвейер с одним объявлением.
    """
    print("\n=== Avito Parser — тестовый прогон (одно объявление) ===\n")

    try:
        settings = load_settings()
    except ConfigValidationError as e:
        print(f"\n[ОШИБКА КОНФИГУРАЦИИ]\n{e}")
        print("\nПроверьте файл .env (см. .env.example для справки).")
        sys.exit(1)

    setup_logging(
        level=settings.log.level,
        log_file_path=settings.log.file_path,
    )

    trace_id = set_trace_id()

    logger.info(
        "test_application_started",
        trace_id=trace_id,
        category_url=settings.scraper.category_url,
        test_db_path=TEST_DB_PATH,
        test_export_path=TEST_EXPORT_PATH,
    )

    try:
        asyncio.run(run_test_pipeline(settings))
    except KeyboardInterrupt:
        logger.info("test_interrupted_by_user")
        print("\nТестовый скрипт остановлен пользователем (Ctrl+C).")
    except Exception as e:
        logger.critical(
            "test_fatal_error",
            exc_info=True,
            error=str(e),
            error_type=type(e).__name__,
        )
        print(f"\nКритическая ошибка: {e}")
        sys.exit(1)

    logger.info("test_application_finished", trace_id=trace_id)


if __name__ == "__main__":
    main()