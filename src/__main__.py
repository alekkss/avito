"""Точка входа приложения Avito Parser.

Связывает все компоненты системы и запускает полный цикл:
1. Загрузка конфигурации и инициализация логирования.
2. Парсинг объявлений аренды из каталога Avito (последовательно).
3. Параллельный парсинг карточек (N воркеров с разными прокси).
4. Экспорт результатов в Excel для анализа.

Запуск: python -m src
"""

import asyncio
import sys
import time

from src.config import (
    ConfigValidationError,
    Settings,
    get_logger,
    load_settings,
    set_trace_id,
    setup_logging,
)
from src.repositories import SQLiteListingRepository
from src.services import (
    BrowserService,
    CatalogItemForWorker,
    ExportService,
    ListingService,
    ParallelListingService,
    ScraperService,
)
from src.services.scraper_service import CatalogItem

logger = get_logger("main")


def create_repository(settings: Settings) -> SQLiteListingRepository:
    """Создаёт и инициализирует репозиторий.

    Фабричная функция (паттерн Factory) для создания
    репозитория с полной инициализацией таблиц.

    Args:
        settings: Настройки приложения.

    Returns:
        Инициализированный SQLite-репозиторий.
    """
    repository = SQLiteListingRepository(
        db_path=settings.database.db_path,
    )
    repository.initialize()
    return repository


def create_browser_service(settings: Settings) -> BrowserService:
    """Создаёт сервис управления браузером для обхода каталога.

    Этот BrowserService используется только на этапе каталога
    (последовательный обход). Для параллельного парсинга карточек
    ParallelListingService создаёт свои BrowserService-ы.

    Args:
        settings: Настройки приложения.

    Returns:
        Экземпляр BrowserService.
    """
    return BrowserService(
        settings=settings.browser,
        proxy_settings=settings.proxy,
    )


def create_listing_service(
    browser_service: BrowserService,
) -> ListingService:
    """Создаёт сервис парсинга карточек объявлений.

    Используется только для последовательного режима (scrape_all).
    В параллельном режиме ListingService создаётся внутри
    ParallelListingService для каждого воркера.

    Args:
        browser_service: Сервис браузера.

    Returns:
        Экземпляр ListingService.
    """
    return ListingService(browser_service=browser_service)


def create_scraper_service(
    browser_service: BrowserService,
    listing_service: ListingService,
    repository: SQLiteListingRepository,
    settings: Settings,
) -> ScraperService:
    """Создаёт сервис парсинга каталога.

    Args:
        browser_service: Сервис браузера.
        listing_service: Сервис парсинга карточек.
        repository: Репозиторий объявлений.
        settings: Настройки приложения.

    Returns:
        Экземпляр ScraperService.
    """
    return ScraperService(
        browser_service=browser_service,
        listing_service=listing_service,
        repository=repository,
        settings=settings.scraper,
    )


def create_parallel_listing_service(
    settings: Settings,
    repository: SQLiteListingRepository,
) -> ParallelListingService:
    """Создаёт сервис параллельной обработки карточек.

    Args:
        settings: Настройки приложения.
        repository: Репозиторий объявлений.

    Returns:
        Экземпляр ParallelListingService.
    """
    return ParallelListingService(
        browser_settings=settings.browser,
        proxy_settings=settings.proxy,
        repository=repository,
    )


def create_export_service(
    repository: SQLiteListingRepository,
    settings: Settings,
) -> ExportService:
    """Создаёт сервис экспорта в Excel.

    Args:
        repository: Репозиторий объявлений.
        settings: Настройки приложения.

    Returns:
        Экземпляр ExportService.
    """
    return ExportService(
        repository=repository,
        settings=settings.export,
    )


def convert_catalog_items(
    catalog_items: list[CatalogItem],
) -> list[CatalogItemForWorker]:
    """Конвертирует CatalogItem из ScraperService в CatalogItemForWorker.

    ScraperService возвращает CatalogItem (свой внутренний формат).
    ParallelListingService принимает CatalogItemForWorker (свой формат).
    Эта функция выполняет маппинг между ними и добавляет нумерацию
    для логирования прогресса.

    Args:
        catalog_items: Список элементов из каталога.

    Returns:
        Список элементов для параллельной обработки.
    """
    total = len(catalog_items)
    return [
        CatalogItemForWorker(
            external_id=item.external_id,
            url=item.url,
            title=item.title,
            price=item.price,
            is_instant_book=item.is_instant_book,
            host_rating=item.host_rating,
            index=index,
            total=total,
        )
        for index, item in enumerate(catalog_items, start=1)
    ]


async def run_pipeline(settings: Settings) -> None:
    """Запускает полный конвейер обработки данных.

    Последовательно выполняет три этапа с замером времени:
    1. Обход каталога Avito (последовательно, один браузер).
    2. Параллельный парсинг карточек (N воркеров с разными прокси).
    3. Экспорт результатов в Excel.

    В конце выводит итоговую сводку по времени и результатам.
    Гарантирует корректное закрытие всех ресурсов
    через блоки try/finally.

    Args:
        settings: Полностью валидированные настройки приложения.
    """
    pipeline_start = time.monotonic()
    catalog_elapsed: float = 0.0
    parsing_elapsed: float = 0.0
    export_elapsed: float = 0.0
    catalog_count: int = 0
    listings_successful: int = 0
    listings_in_db: int = 0
    export_path_result: str | None = None

    repository = create_repository(settings)
    browser_service = create_browser_service(settings)

    try:
        # === ЭТАП 1: Обход каталога ===
        stage_start = time.monotonic()

        logger.info(
            "stage_started",
            stage="catalog",
            url=settings.scraper.category_url,
            proxy_enabled=bool(settings.proxy.proxy_file_path),
        )

        listing_service = create_listing_service(
            browser_service=browser_service,
        )

        scraper_service = create_scraper_service(
            browser_service=browser_service,
            listing_service=listing_service,
            repository=repository,
            settings=settings,
        )

        catalog_items = await scraper_service.scrape_catalog()
        catalog_elapsed = time.monotonic() - stage_start
        catalog_count = len(catalog_items)

        logger.info(
            "stage_completed",
            stage="catalog",
            catalog_items=catalog_count,
            elapsed=round(catalog_elapsed, 1),
        )

        if not catalog_items:
            logger.warning(
                "pipeline_stopped",
                reason="no_catalog_items",
            )
            return

        # Закрываем каталожный браузер — он больше не нужен,
        # воркеры создадут свои контексты
        await browser_service.close()
        logger.info("catalog_browser_closed")

        # === ЭТАП 2: Параллельный парсинг карточек ===
        stage_start = time.monotonic()

        logger.info(
            "stage_started",
            stage="parallel_parsing",
            total_items=catalog_count,
            max_workers=settings.proxy.max_workers,
            proxy_enabled=bool(settings.proxy.proxy_file_path),
            rotate_every=settings.proxy.rotate_every_n,
        )

        worker_items = convert_catalog_items(catalog_items)

        parallel_service = create_parallel_listing_service(
            settings=settings,
            repository=repository,
        )

        listings = await parallel_service.process_all(worker_items)
        parsing_elapsed = time.monotonic() - stage_start
        listings_successful = len(listings)

        listings_in_db = repository.get_listings_count()
        logger.info(
            "stage_completed",
            stage="parallel_parsing",
            new_listings=listings_successful,
            total_in_db=listings_in_db,
            elapsed=round(parsing_elapsed, 1),
        )

        # === ЭТАП 3: Экспорт в Excel ===
        stage_start = time.monotonic()

        logger.info("stage_started", stage="export")

        export_service = create_export_service(
            repository=repository,
            settings=settings,
        )

        export_path_result = export_service.export()
        export_elapsed = time.monotonic() - stage_start

        if export_path_result:
            logger.info(
                "stage_completed",
                stage="export",
                file_path=export_path_result,
                elapsed=round(export_elapsed, 1),
            )
        else:
            logger.warning(
                "export_skipped",
                reason="no_listings_in_database",
            )

    finally:
        # browser_service может быть уже закрыт после этапа каталога,
        # но close() безопасен для повторного вызова
        await browser_service.close()
        repository.close()
        logger.info("all_resources_closed")

    # === ИТОГОВАЯ СВОДКА ===
    pipeline_elapsed = time.monotonic() - pipeline_start

    logger.info(
        "pipeline_summary",
        total_elapsed=round(pipeline_elapsed, 1),
        catalog_elapsed=round(catalog_elapsed, 1),
        parsing_elapsed=round(parsing_elapsed, 1),
        export_elapsed=round(export_elapsed, 1),
        catalog_items=catalog_count,
        listings_successful=listings_successful,
        total_in_db=listings_in_db,
    )

    print(
        f"\n{'═' * 65}"
        f"\n  ИТОГИ ЗАПУСКА"
        f"\n{'═' * 65}"
        f"\n  Этап 1 — Каталог:          "
        f"{catalog_elapsed:>7.1f} сек "
        f"({catalog_elapsed / 60:.1f} мин) "
        f"| {catalog_count} объявлений"
        f"\n  Этап 2 — Парсинг карточек: "
        f"{parsing_elapsed:>7.1f} сек "
        f"({parsing_elapsed / 60:.1f} мин) "
        f"| {listings_successful} успешно"
        f"\n  Этап 3 — Экспорт Excel:    "
        f"{export_elapsed:>7.1f} сек"
        f"\n{'─' * 65}"
        f"\n  Общее время:               "
        f"{pipeline_elapsed:>7.1f} сек "
        f"({pipeline_elapsed / 60:.1f} мин)"
        f"\n  Всего в базе данных:       "
        f"{listings_in_db} объявлений"
    )

    if export_path_result:
        print(
            f"  Excel-файл:                "
            f"{export_path_result}"
        )

    print(f"{'═' * 65}\n")


def main() -> None:
    """Главная функция приложения.

    Загружает конфигурацию, настраивает логирование,
    устанавливает trace_id и запускает асинхронный конвейер.
    Обрабатывает все верхнеуровневые ошибки.
    """
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
    app_start = time.monotonic()

    logger.info(
        "application_started",
        trace_id=trace_id,
        category_url=settings.scraper.category_url,
        max_pages=settings.scraper.max_pages,
        proxy_enabled=bool(settings.proxy.proxy_file_path),
        rotate_every=settings.proxy.rotate_every_n,
        max_workers=settings.proxy.max_workers,
    )

    try:
        asyncio.run(run_pipeline(settings))
    except KeyboardInterrupt:
        logger.info("application_interrupted_by_user")
        print("\nПрограмма остановлена пользователем (Ctrl+C).")
    except Exception as e:
        logger.critical(
            "application_fatal_error",
            exc_info=True,
            error=str(e),
            error_type=type(e).__name__,
        )
        print(f"\nКритическая ошибка: {e}")
        sys.exit(1)

    total_app_elapsed = time.monotonic() - app_start

    logger.info(
        "application_finished",
        trace_id=trace_id,
        total_elapsed=round(total_app_elapsed, 1),
    )


if __name__ == "__main__":
    main()
