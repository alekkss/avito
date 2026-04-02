"""Тестовый скрипт для парсинга одного объявления.

Прогоняет полный pipeline проекта (каталог → карточка → БД → Excel),
но обрабатывает только первое объявление из каталога.
Использует отдельную тестовую БД и отдельный Excel-файл,
чтобы не затрагивать основные данные.

Запуск:
    python scripts/test_single_listing.py
"""

import asyncio
import sys
from pathlib import Path

# Добавляем корень проекта в sys.path, чтобы импорты src.* работали
# независимо от того, откуда запущен скрипт
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

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

logger = get_logger("test_single_listing")


async def run_test_pipeline(settings: Settings) -> None:
    """Запускает тестовый конвейер: парсинг одного объявления.

    Этапы:
    1. Открывает браузер и переходит на каталог Avito.
    2. Парсит карточки на первой странице каталога.
    3. Берёт только первое объявление.
    4. Заходит в его карточку и извлекает детальные данные.
    5. Сохраняет в тестовую БД SQLite.
    6. Экспортирует в тестовый Excel-файл.

    Args:
        settings: Валидированные настройки приложения.
    """
    # --- Инициализация компонентов с тестовыми путями ---
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
            settings.scraper.category_url,
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

        logger.info(
            "test_stage_completed",
            stage="browser_launch",
        )

        # === ЭТАП 2: Парсинг первой страницы каталога ===
        logger.info(
            "test_stage_started",
            stage="catalog_parse",
        )

        listing_service = ListingService(browser_service=browser_service)

        scraper_service = ScraperService(
            browser_service=browser_service,
            listing_service=listing_service,
            repository=repository,
            settings=settings.scraper,
        )

        # Парсим карточки на текущей (первой) странице каталога
        catalog_items = await scraper_service._parse_current_page(page)

        if not catalog_items:
            logger.error("test_no_catalog_items")
            print("\n[ОШИБКА] На странице каталога не найдено ни одного объявления.")
            print("Возможные причины:")
            print("  - Avito показала CAPTCHA или заглушку")
            print("  - Изменились CSS-селекторы каталога")
            print("  - Страница не успела загрузиться")
            return

        # Берём только первое объявление
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
        print(f"Выбрано первое объявление:")
        print(f"  ID:                {first_item.avito_id}")
        print(f"  Название:          {first_item.title}")
        print(f"  Цена:              {first_item.price} руб./сут.")
        print(f"  Мгновенная бронь:  {'Да' if first_item.is_instant_book else 'Нет'}")
        print(f"  Рейтинг (каталог): {first_item.host_rating}")
        print(f"  Ссылка:            https://www.avito.ru{first_item.url}")
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

        # Вывод результатов парсинга карточки
        print(f"Результаты парсинга карточки:")
        print(f"  External ID:             {listing.external_id}")
        print(f"  Название:                {listing.title}")
        print(f"  Категория жилья:         {listing.room_category.value}")
        print(f"  Координаты:              {listing.latitude}, {listing.longitude}")
        print(f"  Средняя цена (руб./сут): {round(listing.average_price)}")
        print(f"  Занятость (%):           {listing.occupancy_rate:.1%}")
        print(f"  Мин. срок (сут.):        {listing.min_stay}")
        print(f"  Мгновенное бронирование: {'Да' if listing.is_instant_book else 'Нет'}")
        print(f"  Рейтинг хоста:           {listing.host_rating}")
        print(f"  Последнее обновление:    {listing.last_host_update or 'Не найдено'}")
        print(f"  Цены (первые 7 дней):    {listing.price_60_days[:7]}")
        print(f"  Календарь (14 дней):     {listing.calendar_60_days[:14]}")
        print()

        # === ЭТАП 4: Сохранение в тестовую БД ===
        logger.info(
            "test_stage_started",
            stage="database_save",
        )

        repository.save_listing(listing)

        db_count = repository.get_listings_count()
        logger.info(
            "test_stage_completed",
            stage="database_save",
            total_in_db=db_count,
        )
        print(f"Сохранено в тестовую БД: {TEST_DB_PATH} (записей: {db_count})")

        # === ЭТАП 5: Экспорт в тестовый Excel ===
        logger.info(
            "test_stage_started",
            stage="excel_export",
        )

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
