"""Точка входа приложения Avito Parser.

Связывает все компоненты системы и запускает полный цикл:
1. Загрузка конфигурации и инициализация логирования.
2. Парсинг товаров из каталога Avito.
3. Нормализация названий через AI (OpenRouter).
4. Экспорт результатов в Excel для анализа цен.

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
from src.repositories import SQLiteProductRepository
from src.services import (
    AIService,
    BrowserService,
    ExportService,
    NormalizerService,
    ScraperService,
)

logger = get_logger("main")


def create_repository(settings: Settings) -> SQLiteProductRepository:
    """Создаёт и инициализирует репозиторий.

    Фабричная функция (паттерн Factory) для создания
    репозитория с полной инициализацией таблиц.

    Args:
        settings: Настройки приложения.

    Returns:
        Инициализированный SQLite-репозиторий.
    """
    repository = SQLiteProductRepository(
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


def create_scraper_service(
    browser_service: BrowserService,
    repository: SQLiteProductRepository,
    settings: Settings,
) -> ScraperService:
    """Создаёт сервис парсинга.

    Args:
        browser_service: Сервис браузера.
        repository: Репозиторий товаров.
        settings: Настройки приложения.

    Returns:
        Экземпляр ScraperService.
    """
    return ScraperService(
        browser_service=browser_service,
        repository=repository,
        settings=settings.scraper,
    )


def create_ai_service(settings: Settings) -> AIService:
    """Создаёт клиент AI API.

    Args:
        settings: Настройки приложения.

    Returns:
        Экземпляр AIService.
    """
    return AIService(settings=settings.ai)


def create_normalizer_service(
    ai_service: AIService,
    repository: SQLiteProductRepository,
    settings: Settings,
) -> NormalizerService:
    """Создаёт сервис нормализации.

    Args:
        ai_service: Клиент AI API.
        repository: Репозиторий товаров.
        settings: Настройки приложения.

    Returns:
        Экземпляр NormalizerService.
    """
    return NormalizerService(
        ai_service=ai_service,
        repository=repository,
        ai_settings=settings.ai,
    )


def create_export_service(
    repository: SQLiteProductRepository,
    settings: Settings,
) -> ExportService:
    """Создаёт сервис экспорта в Excel.

    Args:
        repository: Репозиторий товаров.
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

    Последовательно выполняет три этапа:
    1. Парсинг товаров с Avito (браузер + скрапинг).
    2. Нормализация названий через AI.
    3. Экспорт результатов в Excel.

    Гарантирует корректное закрытие всех ресурсов
    через блоки try/finally.

    Args:
        settings: Полностью валидированные настройки приложения.
    """
    repository = create_repository(settings)
    browser_service = create_browser_service(settings)
    ai_service = create_ai_service(settings)

    try:
        # === ЭТАП 1: Парсинг ===
        logger.info(
            "stage_started",
            stage="scraping",
            url=settings.scraper.category_url,
        )

        scraper_service = create_scraper_service(
            browser_service=browser_service,
            repository=repository,
            settings=settings,
        )

        raw_products = await scraper_service.scrape_all()

        raw_count = repository.get_raw_products_count()
        logger.info(
            "stage_completed",
            stage="scraping",
            new_products=len(raw_products),
            total_in_db=raw_count,
        )

        # === ЭТАП 2: AI-нормализация ===
        logger.info("stage_started", stage="normalization")

        normalizer_service = create_normalizer_service(
            ai_service=ai_service,
            repository=repository,
            settings=settings,
        )

        normalized_products = await normalizer_service.normalize_all()

        norm_count = repository.get_normalized_products_count()
        logger.info(
            "stage_completed",
            stage="normalization",
            new_normalized=len(normalized_products),
            total_normalized=norm_count,
        )

        # === ЭТАП 3: Экспорт в Excel ===
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
                reason="no_normalized_products",
            )

    finally:
        await browser_service.close()
        await ai_service.close()
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
        ai_model=settings.ai.model,
        max_pages=settings.scraper.max_pages,
        batch_size=settings.ai.batch_size,
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
