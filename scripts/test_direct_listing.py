"""Тестовый скрипт для парсинга одного объявления по прямой ссылке.

Переходит напрямую на страницу конкретного объявления Avito
(без обхода каталога), извлекает все детальные данные карточки
через ListingService (координаты, календарь, цены через датепикер,
мин. срок, рейтинг, мгновенное бронирование) и экспортирует
результат в отдельные тестовую БД и Excel-файл.

Запуск:
    python scripts/test_direct_listing.py
"""

import asyncio
import re
import sys
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

logger = get_logger("test_direct_listing")


# ============================================================
# Вспомогательные функции: ID и название из URL
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
# Вспомогательные функции: чтение базовой цены
# ============================================================


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
# Основной конвейер
# ============================================================


async def run_direct_pipeline() -> None:
    """Запускает конвейер парсинга одного объявления по прямой ссылке.

    Этапы:
    1. Запускает браузер и переходит на страницу объявления.
    2. Извлекает базовую цену и название со страницы.
    3. Извлекает все детальные данные через ListingService
       (координаты, календарь, цены через датепикер, мин. срок).
    4. Сохраняет в тестовую БД SQLite.
    5. Экспортирует в тестовый Excel-файл.
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

        # === ЭТАП 2: Чтение базовой цены и названия ===
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

        # === ЭТАП 3: Полный парсинг карточки через ListingService ===
        # ListingService.parse_listing() выполняет всё:
        # координаты, календарь, мин. срок, цены через датепикер,
        # рейтинг хоста, мгновенное бронирование.
        print(
            "\n[этап 3] Парсинг карточки объявления "
            "(ListingService — координаты, календарь, цены)..."
        )

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
        print(
            f"    Средняя цена:     "
            f"{round(listing.average_price)} руб./сут."
        )
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
        print(f"    Цены (первые 14): {listing.price_60_days[:14]}")
        print(f"    Календарь (14):   {listing.calendar_60_days[:14]}")

        # === ЭТАП 4: Сохранение в тестовую БД ===
        print(f"\n[этап 4] Сохранение в тестовую БД...")

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

        # === ЭТАП 5: Экспорт в тестовый Excel ===
        print(f"\n[этап 5] Экспорт в Excel...")

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
