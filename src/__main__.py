"""Точка входа приложения Avito Parser.

Связывает все компоненты системы и запускает полный цикл:
1. Загрузка конфигурации и инициализация логирования.
2. Парсинг объявлений аренды из каталога Avito.
3. Детальный парсинг карточек (координаты, календарь, цены).
4. Экспорт результатов в Excel для анализа.

Запуск: python -m src
"""

import asyncio
import sys

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
    ExportService,
    ListingService,
    ScraperService,
)

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
    """Создаёт сервис управления браузером.

    Args:
        settings: Настройки приложения.

    Returns:
        Экземпляр BrowserService.
    """
    return BrowserService(settings=settings.browser)


def create_listing_service(
    browser_service: BrowserService,
) -> ListingService:
    """Создаёт сервис парсинга карточек объявлений.

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


async def run_pipeline(settings: Settings) -> None:
    """Запускает полный конвейер обработки данных.

    Последовательно выполняет два этапа:
    1. Парсинг объявлений аренды с Avito (каталог + карточки).
    2. Экспорт результатов в Excel.

    Гарантирует корректное закрытие всех ресурсов
    через блоки try/finally.

    Args:
        settings: Полностью валидированные настройки приложения.
    """
    repository = create_repository(settings)
    browser_service = create_browser_service(settings)

    try:
        # === ЭТАП 1: Парсинг ===
        logger.info(
            "stage_started",
            stage="scraping",
            url=settings.scraper.category_url,
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

        listings = await scraper_service.scrape_all()

        listings_count = repository.get_listings_count()
        logger.info(
            "stage_completed",
            stage="scraping",
            new_listings=len(listings),
            total_in_db=listings_count,
        )

        # === ЭТАП 2: Экспорт в Excel ===
        logger.info("stage_started", stage="export")

        export_service = create_export_service(
            repository=repository,
            settings=settings,
        )

        export_path = export_service.export()

        if export_path:
            logger.info(
                "stage_completed",
                stage="export",
                file_path=export_path,
            )
        else:
            logger.warning(
                "export_skipped",
                reason="no_listings_in_database",
            )

    finally:
        await browser_service.close()
        repository.close()
        logger.info("all_resources_closed")


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

    logger.info(
        "application_started",
        trace_id=trace_id,
        category_url=settings.scraper.category_url,
        max_pages=settings.scraper.max_pages,
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

    logger.info("application_finished", trace_id=trace_id)


if __name__ == "__main__":
    main()
