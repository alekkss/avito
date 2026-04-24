"""Тестовый скрипт: 3 параллельных потока по 10 объявлений.

Каждый поток использует свой прокси и свою браузерную сессию.
При провале хотя бы одной карточки — неспарсенные объявления
передаются новому потоку с новым прокси и новой сессией.

Полный цикл:
1. Загрузка конфигурации и прокси из .env / proxies.txt.
2. Парсинг каталога одним браузером (первый прокси) → 30 объявлений.
3. Разбивка на 3 группы по 10 объявлений.
4. Запуск 3 параллельных воркеров (asyncio.gather):
   - каждый воркер: свой прокси → свой BrowserContext → прогрев
     сессии → парсинг 10 карточек с имитацией поведения.
5. Сбор результатов. Если есть проваленные карточки —
   failover-раунд: новый воркер с новым прокси на неспарсенные.
6. Сохранение в тестовую БД (data/test_10_listings.db).
7. Экспорт в тестовый Excel (data/test_10_listings.xlsx).

Требования:
- Минимум 3 прокси в файле proxies.txt (+ дополнительные для failover).
- Формат прокси: host:port:user:pass (по одному на строку).

Запуск:
    python scripts/test_10_listings.py
"""

import asyncio
import random
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

from playwright.async_api import Page, Route

# Добавляем корень проекта в sys.path
_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.config import (
    ConfigValidationError,
    DatabaseSettings,
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
from src.services.browser_service import ProxyInfo, load_proxies_from_file
from src.services.scraper_service import CatalogItem

# --- Константы ---

# Количество параллельных воркеров
NUM_WORKERS: int = 8

# Объявлений на один воркер
LISTINGS_PER_WORKER: int = 10

# Итого объявлений из каталога
TOTAL_LISTINGS: int = NUM_WORKERS * LISTINGS_PER_WORKER

# Максимальное количество failover-раундов
MAX_FAILOVER_ROUNDS: int = 3

# Пути к тестовым файлам
TEST_DB_PATH: str = "data/test_10_listings.db"
TEST_EXPORT_PATH: str = "data/test_10_listings.xlsx"

# --- Настройки скрытности ---

# Паузы между карточками (секунды)
CARD_DELAY_MIN: float = 8.0
CARD_DELAY_MAX: float = 15.0

# Паузы между прогревочными страницами (секунды)
WARMUP_DELAY_MIN: float = 3.0
WARMUP_DELAY_MAX: float = 6.0

# Паттерны URL для блокировки тяжёлых/трекерных ресурсов
BLOCKED_RESOURCE_PATTERNS: list[str] = [
    "mc.yandex.ru",
    "yandex.ru/metrika",
    "google-analytics.com",
    "googletagmanager.com",
    "doubleclick.net",
    "facebook.net",
    "facebook.com/tr",
    "vk.com/rtrg",
    "top-fwz1.mail.ru",
    "top.mail.ru",
    "counter.yadro.ru",
    "adfox.yandex.ru",
    "an.yandex.ru",
    "ads.adfox.ru",
    "sentry.io",
    "hotjar.com",
]

# URL для прогрева сессии
WARMUP_URLS: list[str] = [
    "https://www.avito.ru",
    "https://www.avito.ru/sankt-peterburg",
    "https://www.avito.ru/sankt-peterburg/kvartiry",
]


@dataclass
class WorkerResult:
    """Результат работы одного воркера.

    Attributes:
        worker_id: Идентификатор воркера (1, 2, 3...).
        proxy_server: Адрес использованного прокси.
        successful: Список external_id успешно спарсенных карточек.
        failed_items: Список CatalogItem, которые не удалось спарсить.
        elapsed: Время работы воркера в секундах.
    """

    worker_id: int
    proxy_server: str
    successful: list[str] = field(default_factory=list)
    failed_items: list[CatalogItem] = field(default_factory=list)
    elapsed: float = 0.0


# ==================================================================
# Утилиты скрытности
# ==================================================================


async def _block_heavy_resources(route: Route) -> None:
    """Блокирует тяжёлые и трекерные ресурсы.

    Вызывается Playwright для каждого HTTP-запроса. Блокирует
    изображения, шрифты, медиа и запросы к аналитическим сервисам.

    Args:
        route: Объект маршрута Playwright для перехвата запросов.
    """
    request = route.request
    resource_type = request.resource_type
    url = request.url.lower()

    if resource_type in ("image", "font", "media"):
        await route.abort()
        return

    for pattern in BLOCKED_RESOURCE_PATTERNS:
        if pattern in url:
            await route.abort()
            return

    await route.continue_()


async def setup_route_blocking(page: Page) -> None:
    """Настраивает перехват и блокировку ресурсов на странице.

    Args:
        page: Страница Playwright для настройки блокировки.
    """
    await page.route("**/*", _block_heavy_resources)


async def warmup_session(
    page: Page,
    browser_service: BrowserService,
    worker_id: int,
) -> None:
    """Прогревает сессию браузера перед парсингом карточек.

    Заходит на несколько «нейтральных» страниц Avito для набора
    cookies и создания «истории» навигации.

    Args:
        page: Активная страница Playwright.
        browser_service: Сервис браузера для имитации поведения.
        worker_id: ID воркера для логирования.
    """
    logger = get_logger("test_10_listings")
    prefix = f"worker_{worker_id}"

    logger.info(
        "warmup_session_started",
        source=prefix,
        warmup_pages=len(WARMUP_URLS),
    )

    for idx, url in enumerate(WARMUP_URLS, start=1):
        try:
            delay = random.uniform(WARMUP_DELAY_MIN, WARMUP_DELAY_MAX)
            logger.debug(
                "warmup_page_navigating",
                source=prefix,
                url=url[:80],
                delay=round(delay, 1),
                step=f"{idx}/{len(WARMUP_URLS)}",
            )

            await asyncio.sleep(delay)

            await page.goto(
                url,
                wait_until="domcontentloaded",
                timeout=60000,
            )
            await asyncio.sleep(random.uniform(2.0, 4.0))
            await browser_service.simulate_human_behavior()

        except Exception as e:
            logger.warning(
                "warmup_page_failed",
                source=prefix,
                url=url[:80],
                error=str(e),
            )

    logger.info("warmup_session_completed", source=prefix)


async def simulate_reading_listing(
    page: Page,
    browser_service: BrowserService,
) -> None:
    """Имитирует чтение объявления реальным пользователем.

    Args:
        page: Активная страница Playwright.
        browser_service: Сервис браузера для simulate_human_behavior.
    """
    try:
        await page.evaluate(
            "window.scrollTo({top: document.body.scrollHeight / 3, "
            "behavior: 'smooth'})"
        )
        await asyncio.sleep(random.uniform(1.0, 2.5))

        await page.evaluate(
            "window.scrollTo({top: document.body.scrollHeight / 2, "
            "behavior: 'smooth'})"
        )
        await asyncio.sleep(random.uniform(1.0, 2.0))

        await page.evaluate(
            "window.scrollTo({top: 0, behavior: 'smooth'})"
        )
        await asyncio.sleep(random.uniform(1.0, 2.0))

        await browser_service.simulate_human_behavior()

    except Exception:
        pass


# ==================================================================
# Настройки
# ==================================================================


def build_test_settings(original: Settings) -> Settings:
    """Создаёт тестовые настройки на основе оригинальных.

    Каталог парсится одной страницей — на ней обычно 50 объявлений,
    из которых мы берём первые 30. Пути к БД и Excel — тестовые.

    Args:
        original: Оригинальные настройки из .env.

    Returns:
        Настройки с тестовыми путями.
    """
    from dataclasses import replace

    return Settings(
        browser=original.browser,
        scraper=replace(original.scraper, max_pages=1),
        database=DatabaseSettings(db_path=TEST_DB_PATH),
        export=ExportSettings(export_path=TEST_EXPORT_PATH),
        log=original.log,
        proxy=original.proxy,
    )


def load_and_validate_proxies(settings: Settings) -> list[ProxyInfo]:
    """Загружает и валидирует список прокси.

    Для работы скрипта нужно минимум NUM_WORKERS (3) прокси.
    Дополнительные прокси используются для failover-раундов.

    Args:
        settings: Настройки приложения.

    Returns:
        Список прокси.

    Raises:
        RuntimeError: Если прокси недостаточно.
    """
    if not settings.proxy.proxy_file_path:
        raise RuntimeError(
            f"Для работы тестового скрипта необходимо минимум "
            f"{NUM_WORKERS} прокси. Укажите PROXY_FILE_PATH в .env."
        )

    proxies = load_proxies_from_file(settings.proxy.proxy_file_path)

    if len(proxies) < NUM_WORKERS:
        raise RuntimeError(
            f"Недостаточно прокси: найдено {len(proxies)}, "
            f"требуется минимум {NUM_WORKERS}. "
            f"Добавьте прокси в файл "
            f"{settings.proxy.proxy_file_path}."
        )

    return proxies


# ==================================================================
# Воркер — полный цикл парсинга на одном прокси
# ==================================================================


async def run_worker(
    worker_id: int,
    proxy: ProxyInfo,
    items: list[CatalogItem],
    settings: Settings,
    repository: SQLiteListingRepository,
) -> WorkerResult:
    """Запускает полный цикл парсинга для одного воркера.

    Создаёт свою браузерную сессию с назначенным прокси,
    прогревает сессию, парсит карточки последовательно
    с увеличенными паузами и имитацией поведения.

    Args:
        worker_id: Идентификатор воркера (для логов).
        proxy: Прокси для этого воркера.
        items: Список CatalogItem для парсинга.
        settings: Настройки приложения.
        repository: Репозиторий (общий для всех воркеров).

    Returns:
        WorkerResult с результатами парсинга.
    """
    logger = get_logger("test_10_listings")
    prefix = f"worker_{worker_id}"
    result = WorkerResult(
        worker_id=worker_id,
        proxy_server=proxy.server,
    )
    start_time = time.monotonic()

    # Создаём BrowserService с единственным прокси —
    # ротация внутри воркера не нужна, весь цикл на одном IP
    browser_service = BrowserService(
        settings=settings.browser,
        proxy_settings=settings.proxy,
        assigned_proxies=[proxy],
        worker_id=worker_id,
    )

    listing_service = ListingService(browser_service=browser_service)

    try:
        # === Запуск браузера ===
        logger.info(
            "worker_starting",
            source=prefix,
            proxy=proxy.server,
            items_count=len(items),
        )
        print(
            f"\n  [{prefix}] Запуск | прокси: {proxy.server} "
            f"| карточек: {len(items)}"
        )

        page = await browser_service.launch()

        # === Настройка скрытности ===
        await setup_route_blocking(page)

        # === Прогрев сессии ===
        print(f"  [{prefix}] 🔥 Прогрев сессии...")
        await warmup_session(page, browser_service, worker_id)

        # === Парсинг карточек ===
        print(
            f"  [{prefix}] 📋 Парсинг {len(items)} карточек "
            f"(паузы {CARD_DELAY_MIN:.0f}-{CARD_DELAY_MAX:.0f} сек)"
        )

        current_page = browser_service.page or page

        for index, item in enumerate(items, start=1):
            # Пауза между карточками (кроме первой)
            if index > 1:
                card_delay = random.uniform(
                    CARD_DELAY_MIN, CARD_DELAY_MAX,
                )
                logger.debug(
                    "worker_inter_card_delay",
                    source=prefix,
                    delay=round(card_delay, 1),
                    before_card=index,
                )
                await asyncio.sleep(card_delay)

            logger.info(
                "worker_parsing_listing",
                source=prefix,
                progress=f"{index}/{len(items)}",
                external_id=item.external_id,
                title=item.title[:50],
            )
            print(
                f"  [{prefix}] [{index}/{len(items)}] "
                f"{item.external_id}: {item.title[:50]}"
            )

            # Парсинг карточки
            listing = await listing_service.parse_listing(
                page=current_page,
                external_id=item.external_id,
                url=item.url,
                title=item.title,
                base_price=item.price,
                is_instant_book=item.is_instant_book,
                catalog_host_rating=item.host_rating,
            )

            if listing is not None:
                # Имитация «чтения» после парсинга
                await simulate_reading_listing(
                    current_page, browser_service,
                )

                # Сохранение через asyncio-безопасный метод
                await repository.save_listing_async(listing)
                result.successful.append(item.external_id)

                logger.info(
                    "worker_listing_saved",
                    source=prefix,
                    progress=f"{index}/{len(items)}",
                    external_id=listing.external_id,
                    room_category=listing.room_category.value,
                    avg_price=round(listing.average_price),
                    occupancy=f"{listing.occupancy_rate:.0%}",
                )
                print(
                    f"  [{prefix}]   ✅ "
                    f"{listing.room_category.value}"
                    f" | {round(listing.average_price)} руб."
                    f" | {listing.occupancy_rate:.0%} занятость"
                )
            else:
                result.failed_items.append(item)
                logger.warning(
                    "worker_listing_failed",
                    source=prefix,
                    progress=f"{index}/{len(items)}",
                    external_id=item.external_id,
                )
                print(
                    f"  [{prefix}]   ❌ Не удалось спарсить"
                )

            # Обновляем current_page (мог измениться при ротации)
            if browser_service.page is not None:
                current_page = browser_service.page

    except Exception as e:
        logger.error(
            "worker_fatal_error",
            source=prefix,
            exc_info=True,
            error=str(e),
        )
        print(f"  [{prefix}] 💥 Критическая ошибка: {e}")

        # Все необработанные карточки — в проваленные
        processed_ids = set(result.successful) | {
            item.external_id for item in result.failed_items
        }
        for item in items:
            if item.external_id not in processed_ids:
                result.failed_items.append(item)

    finally:
        await browser_service.close()
        result.elapsed = time.monotonic() - start_time

        logger.info(
            "worker_finished",
            source=prefix,
            successful=len(result.successful),
            failed=len(result.failed_items),
            elapsed=round(result.elapsed, 1),
        )
        print(
            f"  [{prefix}] Завершён за {result.elapsed:.1f} сек "
            f"| ✅ {len(result.successful)} "
            f"| ❌ {len(result.failed_items)}"
        )

    return result


# ==================================================================
# Парсинг каталога (один раз, первым прокси)
# ==================================================================


async def parse_catalog(
    settings: Settings,
    proxy: ProxyInfo,
    repository: SQLiteListingRepository,
) -> list[CatalogItem]:
    """Парсит каталог Avito одним браузером с первым прокси.

    Создаёт отдельную браузерную сессию только для каталога.
    После сбора данных сессия закрывается — воркеры создадут свои.

    Args:
        settings: Настройки приложения.
        proxy: Прокси для каталожного браузера.
        repository: Репозиторий (передаётся в ScraperService).

    Returns:
        Список CatalogItem из каталога.
    """
    logger = get_logger("test_10_listings")

    browser_service = BrowserService(
        settings=settings.browser,
        proxy_settings=settings.proxy,
        assigned_proxies=[proxy],
    )

    listing_service = ListingService(browser_service=browser_service)

    scraper_service = ScraperService(
        browser_service=browser_service,
        listing_service=listing_service,
        repository=repository,
        settings=settings.scraper,
    )

    try:
        logger.info(
            "catalog_parsing_started",
            proxy=proxy.server,
            url=settings.scraper.category_url,
        )

        catalog_items = await scraper_service.scrape_catalog()

        logger.info(
            "catalog_parsing_completed",
            total_items=len(catalog_items),
        )

        return catalog_items

    finally:
        await browser_service.close()


# ==================================================================
# Оркестратор — управление воркерами и failover
# ==================================================================


async def run_test_pipeline(settings: Settings) -> None:
    """Запускает тестовый конвейер с параллельными воркерами.

    Полный цикл:
    1. Парсинг каталога (1 прокси) → объявления.
    2. Разбивка на группы по LISTINGS_PER_WORKER.
    3. Запуск воркеров параллельно (asyncio.gather).
    4. Сбор результатов → failover для проваленных карточек.
    5. Экспорт в Excel.

    Args:
        settings: Тестовые настройки приложения.
    """
    logger = get_logger("test_10_listings")

    # === Загрузка прокси ===
    proxies = load_and_validate_proxies(settings)
    proxy_index = 0

    print(
        f"\n{'=' * 65}"
        f"\n  ТЕСТОВЫЙ ПРОГОН: {NUM_WORKERS} потока × "
        f"{LISTINGS_PER_WORKER} объявлений = {TOTAL_LISTINGS} шт."
        f"\n  Прокси в пуле: {len(proxies)} "
        f"({NUM_WORKERS} основных + "
        f"{max(0, len(proxies) - NUM_WORKERS)} для failover)"
        f"\n{'=' * 65}"
    )

    # === Инициализация репозитория ===
    repository = SQLiteListingRepository(
        db_path=settings.database.db_path,
    )
    repository.initialize()

    # Список ВСЕХ результатов воркеров (основные + failover)
    all_worker_results: list[WorkerResult] = []

    try:
        # === ЭТАП 1: Парсинг каталога ===
        print(
            f"\n[1/4] Парсинг каталога "
            f"(прокси: {proxies[proxy_index].server})..."
        )

        catalog_items = await parse_catalog(
            settings=settings,
            proxy=proxies[proxy_index],
            repository=repository,
        )
        proxy_index += 1

        if not catalog_items:
            print("\n  Объявления в каталоге не найдены. Завершение.")
            return

        # Берём первые TOTAL_LISTINGS объявлений
        total_in_catalog = len(catalog_items)
        catalog_items = catalog_items[:TOTAL_LISTINGS]

        print(
            f"  Найдено в каталоге: {total_in_catalog}"
            f"\n  Выбрано для теста: {len(catalog_items)}"
        )

        # === ЭТАП 2: Разбивка на группы ===
        groups: list[list[CatalogItem]] = []
        for i in range(0, len(catalog_items), LISTINGS_PER_WORKER):
            group = catalog_items[i:i + LISTINGS_PER_WORKER]
            if group:
                groups.append(group)

        print(
            f"\n[2/4] Разбивка: {len(groups)} групп"
            f" по {LISTINGS_PER_WORKER} объявлений"
        )
        for idx, group in enumerate(groups, start=1):
            print(
                f"  Группа {idx}: {len(group)} объявлений "
                f"({group[0].external_id} ... "
                f"{group[-1].external_id})"
            )

        # === ЭТАП 3: Параллельный парсинг + failover ===
        print(
            f"\n[3/4] Запуск {len(groups)} параллельных воркеров..."
        )

        all_successful: list[str] = []
        all_failed: list[CatalogItem] = []
        failover_round = 0

        # --- Основной раунд ---
        tasks: list[asyncio.Task[WorkerResult]] = []
        for group_idx, group in enumerate(groups):
            if proxy_index >= len(proxies):
                logger.error(
                    "not_enough_proxies_for_workers",
                    needed=len(groups),
                    available=len(proxies),
                )
                print(
                    f"\n  ⚠️  Недостаточно прокси для всех воркеров."
                    f" Доступно: {len(proxies)}, нужно: "
                    f"{proxy_index + 1}"
                )
                # Помечаем все карточки группы как проваленные
                all_failed.extend(group)
                continue

            worker_id = group_idx + 1
            proxy = proxies[proxy_index]
            proxy_index += 1

            task = asyncio.create_task(
                run_worker(
                    worker_id=worker_id,
                    proxy=proxy,
                    items=group,
                    settings=settings,
                    repository=repository,
                ),
                name=f"worker_{worker_id}",
            )
            tasks.append(task)

        # Ожидаем завершения всех воркеров
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Собираем результаты
        worker_id_counter = len(groups)
        for r in results:
            if isinstance(r, Exception):
                logger.error(
                    "worker_exception",
                    error=str(r),
                    error_type=type(r).__name__,
                )
                continue

            all_worker_results.append(r)
            all_successful.extend(r.successful)
            all_failed.extend(r.failed_items)

        _print_round_summary(
            round_name="Основной раунд",
            successful=len(all_successful),
            failed=len(all_failed),
        )

        # --- Failover-раунды ---
        while all_failed and failover_round < MAX_FAILOVER_ROUNDS:
            failover_round += 1

            if proxy_index >= len(proxies):
                logger.warning(
                    "no_proxies_left_for_failover",
                    round=failover_round,
                    failed_count=len(all_failed),
                    proxies_used=proxy_index,
                    proxies_total=len(proxies),
                )
                print(
                    f"\n  ⚠️  Нет свободных прокси для "
                    f"failover-раунда {failover_round}. "
                    f"Неспарсенных: {len(all_failed)}"
                )
                break

            proxy = proxies[proxy_index]
            proxy_index += 1
            worker_id_counter += 1

            failed_ids = [
                item.external_id for item in all_failed
            ]
            logger.info(
                "failover_round_started",
                round=failover_round,
                failed_count=len(all_failed),
                failed_ids=failed_ids,
                proxy=proxy.server,
            )
            print(
                f"\n  🔄 Failover-раунд {failover_round}: "
                f"{len(all_failed)} карточек → "
                f"прокси {proxy.server}"
            )

            # Передаём все проваленные одному новому воркеру
            retry_items = list(all_failed)
            all_failed = []

            failover_result = await run_worker(
                worker_id=worker_id_counter,
                proxy=proxy,
                items=retry_items,
                settings=settings,
                repository=repository,
            )

            all_worker_results.append(failover_result)
            all_successful.extend(failover_result.successful)
            all_failed.extend(failover_result.failed_items)

            _print_round_summary(
                round_name=f"Failover-раунд {failover_round}",
                successful=len(failover_result.successful),
                failed=len(failover_result.failed_items),
            )

        # === ЭТАП 4: Экспорт в Excel ===
        print("\n[4/4] Экспорт в Excel...")

        export_service = ExportService(
            repository=repository,
            settings=settings.export,
        )
        export_path = export_service.export()

        if export_path:
            print(f"  Файл сохранён: {export_path}")
        else:
            print("  Нет данных для экспорта.")

        # === Итоги ===
        total_in_db = repository.get_listings_count()
        failed_ids_final = [
            item.external_id for item in all_failed
        ]

        print(
            f"\n{'=' * 65}"
            f"\n  ИТОГИ ТЕСТОВОГО ПРОГОНА"
            f"\n{'=' * 65}"
            f"\n  Объявлений в каталоге:     {total_in_catalog}"
            f"\n  Выбрано для теста:         {TOTAL_LISTINGS}"
            f"\n  Воркеров (основных):       {NUM_WORKERS}"
            f"\n  Failover-раундов:          {failover_round}"
            f"\n  Прокси использовано:       {proxy_index}"
            f"/{len(proxies)}"
            f"\n  Успешно спарсено:          "
            f"{len(all_successful)}"
            f"\n  Не удалось спарсить:       {len(all_failed)}"
            f"\n  Всего в тестовой БД:       {total_in_db}"
            f"\n  Тестовая БД:               {TEST_DB_PATH}"
            f"\n  Тестовый Excel:            {TEST_EXPORT_PATH}"
        )

        # --- Статистика по воркерам ---
        if all_worker_results:
            print(f"\n  {'─' * 61}")
            print("  ВРЕМЯ ВЫПОЛНЕНИЯ ВОРКЕРОВ")
            print(f"  {'─' * 61}")

            # Сортируем по worker_id для наглядности
            sorted_results = sorted(
                all_worker_results, key=lambda r: r.worker_id,
            )

            for wr in sorted_results:
                # Определяем тип: основной или failover
                worker_type = (
                    "основной"
                    if wr.worker_id <= NUM_WORKERS
                    else "failover"
                )
                status_icon = (
                    "✅" if not wr.failed_items else "⚠️ "
                )

                # Среднее время на карточку (если были успешные)
                cards_total = (
                    len(wr.successful) + len(wr.failed_items)
                )
                avg_per_card = (
                    wr.elapsed / cards_total
                    if cards_total > 0
                    else 0.0
                )

                print(
                    f"    {status_icon} worker_{wr.worker_id:>2} "
                    f"({worker_type:>8}): "
                    f"{wr.elapsed:>7.1f} сек "
                    f"| {len(wr.successful):>2} ✅ "
                    f"{len(wr.failed_items):>2} ❌ "
                    f"| ~{avg_per_card:.1f} сек/карточка "
                    f"| {wr.proxy_server}"
                )

            # Сводная статистика
            elapsed_list = [
                wr.elapsed for wr in all_worker_results
            ]
            avg_elapsed = (
                sum(elapsed_list) / len(elapsed_list)
            )
            min_elapsed = min(elapsed_list)
            max_elapsed = max(elapsed_list)

            # Среднее время на карточку по всем воркерам
            total_cards = sum(
                len(wr.successful) + len(wr.failed_items)
                for wr in all_worker_results
            )
            total_time = sum(elapsed_list)
            avg_per_card_global = (
                total_time / total_cards
                if total_cards > 0
                else 0.0
            )

            print(f"\n  {'─' * 61}")
            print(
                f"  Среднее время воркера:     "
                f"{avg_elapsed:.1f} сек"
            )
            print(
                f"  Минимальное:               "
                f"{min_elapsed:.1f} сек"
            )
            print(
                f"  Максимальное:              "
                f"{max_elapsed:.1f} сек"
            )
            print(
                f"  Разброс (макс − мин):      "
                f"{max_elapsed - min_elapsed:.1f} сек"
            )
            print(
                f"  Среднее на карточку:       "
                f"{avg_per_card_global:.1f} сек"
            )

            # Логируем сводку по воркерам
            worker_timings = {
                f"worker_{wr.worker_id}": round(wr.elapsed, 1)
                for wr in sorted_results
            }
            logger.info(
                "workers_timing_summary",
                worker_count=len(all_worker_results),
                avg_elapsed=round(avg_elapsed, 1),
                min_elapsed=round(min_elapsed, 1),
                max_elapsed=round(max_elapsed, 1),
                spread=round(max_elapsed - min_elapsed, 1),
                avg_per_card=round(avg_per_card_global, 1),
                total_cards=total_cards,
                timings=worker_timings,
            )

        if all_failed:
            print(
                f"\n  ⚠️  Неспарсенные карточки: "
                f"{', '.join(failed_ids_final)}"
            )
        print(f"{'=' * 65}\n")

    finally:
        repository.close()
        logger.info("test_all_resources_closed")


def _print_round_summary(
    round_name: str,
    successful: int,
    failed: int,
) -> None:
    """Печатает сводку по результатам раунда.

    Args:
        round_name: Название раунда для вывода.
        successful: Количество успешных карточек.
        failed: Количество проваленных карточек.
    """
    status = "✅" if failed == 0 else "⚠️ "
    print(
        f"\n  {status} {round_name}: "
        f"✅ {successful} успешно | ❌ {failed} провалено"
    )


def main() -> None:
    """Точка входа тестового скрипта.

    Загружает конфигурацию, подменяет пути на тестовые,
    валидирует наличие прокси и запускает конвейер.
    """
    try:
        original_settings = load_settings()
    except ConfigValidationError as e:
        print(f"\n[ОШИБКА КОНФИГУРАЦИИ]\n{e}")
        print(
            "\nПроверьте файл .env (см. .env.example для справки)."
        )
        sys.exit(1)

    test_settings = build_test_settings(original_settings)

    setup_logging(
        level=test_settings.log.level,
        log_file_path=test_settings.log.file_path,
    )

    trace_id = set_trace_id()

    logger = get_logger("test_10_listings")
    logger.info(
        "test_started",
        trace_id=trace_id,
        num_workers=NUM_WORKERS,
        listings_per_worker=LISTINGS_PER_WORKER,
        total_listings=TOTAL_LISTINGS,
        category_url=test_settings.scraper.category_url,
        proxy_file=test_settings.proxy.proxy_file_path,
        test_db=TEST_DB_PATH,
        test_excel=TEST_EXPORT_PATH,
    )

    try:
        asyncio.run(run_test_pipeline(test_settings))
    except KeyboardInterrupt:
        logger.info("test_interrupted_by_user")
        print(
            "\nТестовый прогон остановлен пользователем (Ctrl+C)."
        )
    except RuntimeError as e:
        logger.error(
            "test_runtime_error",
            error=str(e),
        )
        print(f"\n[ОШИБКА] {e}")
        sys.exit(1)
    except Exception as e:
        logger.critical(
            "test_fatal_error",
            exc_info=True,
            error=str(e),
            error_type=type(e).__name__,
        )
        print(f"\nКритическая ошибка: {e}")
        sys.exit(1)

    logger.info("test_finished", trace_id=trace_id)


if __name__ == "__main__":
    main()
