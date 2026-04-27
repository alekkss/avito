"""Сервис параллельной обработки карточек объявлений.

Оркестрирует N параллельных воркеров, каждый из которых использует
собственный BrowserContext с уникальным прокси, User-Agent и viewport.
Карточки распределяются через asyncio.Queue — воркер, освободившийся
первым, берёт следующую карточку (балансировка нагрузки).

Все воркеры разделяют один ProxyHealthTracker — если прокси забанен
у одного воркера, остальные об этом узнают и пропустят его при ротации.

После основного раунда проваленные карточки автоматически
перезапускаются в failover-раундах (до MAX_FAILOVER_ROUNDS попыток)
с использованием оставшихся здоровых прокси.

Паттерн Strategy: ParallelListingService заменяет последовательный
обход карточек в ScraperService._parse_all_listings() на параллельный,
не меняя логику парсинга отдельной карточки (ListingService).
"""

import asyncio
import random
import time
from dataclasses import dataclass, field

from playwright.async_api import Browser, Playwright, async_playwright

from src.config import BrowserSettings, ProxySettings, get_logger
from src.models import RawListing
from src.repositories.sqlite_repository import SQLiteListingRepository
from src.services.browser_service import (
    BROWSER_ARGS,
    BrowserService,
    ProxyInfo,
    load_proxies_from_file,
)
from src.services.listing_service import ListingService
from src.services.proxy_health import ProxyHealthTracker

logger = get_logger("parallel_listing_service")

# Максимальное количество воркеров при автоопределении
MAX_AUTO_WORKERS: int = 10

# Максимальное количество failover-раундов
MAX_FAILOVER_ROUNDS: int = 3

# Пауза между failover-раундами (секунды),
# позволяет Avito «забыть» о предыдущих запросах
FAILOVER_COOLDOWN_MIN: float = 5.0
FAILOVER_COOLDOWN_MAX: float = 10.0

# URL для «прогрева» нового контекста воркера
WARMUP_URL: str = "https://www.avito.ru"

# Минимальная пауза между стартом воркеров (секунды),
# чтобы не создавать одновременную нагрузку
WORKER_STAGGER_DELAY_MIN: float = 2.0
WORKER_STAGGER_DELAY_MAX: float = 5.0

# Пауза между обработкой карточек внутри воркера (секунды),
# имитирует человеческий ритм просмотра объявлений
WORKER_INTER_CARD_DELAY_MIN: float = 1.0
WORKER_INTER_CARD_DELAY_MAX: float = 3.0


@dataclass
class CatalogItemForWorker:
    """Элемент очереди для воркера.

    Содержит все данные, необходимые ListingService.parse_listing()
    для обработки одной карточки объявления.

    Attributes:
        external_id: ID объявления (формат "av_<id>").
        url: Относительная или абсолютная ссылка на объявление.
        title: Название объявления из каталога.
        price: Базовая цена из каталога (руб./сут.).
        is_instant_book: Флаг мгновенного бронирования из каталога.
        host_rating: Рейтинг хоста из каталога.
        index: Порядковый номер карточки (для логирования прогресса).
        total: Общее количество карточек (для логирования прогресса).
    """

    external_id: str
    url: str
    title: str
    price: int
    is_instant_book: bool
    host_rating: float
    index: int
    total: int


@dataclass
class WorkerResult:
    """Результат работы одного воркера.

    Attributes:
        worker_id: Идентификатор воркера.
        successful: Количество успешно обработанных карточек.
        failed: Количество карточек с ошибками.
        elapsed: Время работы воркера в секундах.
        failed_items: Список проваленных карточек для failover.
    """

    worker_id: int
    successful: int
    failed: int
    elapsed: float = 0.0
    failed_items: list[CatalogItemForWorker] = field(
        default_factory=list
    )


def _determine_worker_count(
    proxy_settings: ProxySettings,
    proxy_count: int,
    total_items: int,
) -> int:
    """Определяет оптимальное количество воркеров.

    Логика:
    - Если прокси нет — всегда 1 воркер (последовательная обработка).
    - Если max_workers задан явно (> 0) — используется он,
      но не более количества прокси и не более количества карточек.
    - Если max_workers = 0 (авто) — равно количеству прокси,
      но не более MAX_AUTO_WORKERS и не более количества карточек.

    Args:
        proxy_settings: Настройки прокси из конфигурации.
        proxy_count: Количество загруженных прокси.
        total_items: Общее количество карточек для обработки.

    Returns:
        Оптимальное количество воркеров (минимум 1).
    """
    if proxy_count == 0:
        return 1

    if total_items == 0:
        return 1

    if proxy_settings.max_workers > 0:
        # Явно задано пользователем
        count = proxy_settings.max_workers
    else:
        # Автоопределение
        count = min(proxy_count, MAX_AUTO_WORKERS)

    # Не больше прокси (каждому воркеру нужен хотя бы 1 прокси)
    count = min(count, proxy_count)

    # Не больше карточек (нет смысла в пустых воркерах)
    count = min(count, total_items)

    # Минимум 1
    return max(count, 1)


def _distribute_proxies(
    all_proxies: list[ProxyInfo],
    worker_count: int,
) -> list[list[ProxyInfo]]:
    """Распределяет прокси между воркерами.

    Каждый воркер получает свой набор прокси для ротации.
    Распределение выполняется round-robin: первый прокси
    первому воркеру, второй — второму, и так далее по кругу.
    Это гарантирует, что воркеры стартуют с разных IP.

    Args:
        all_proxies: Полный список прокси.
        worker_count: Количество воркеров.

    Returns:
        Список из worker_count списков ProxyInfo.
    """
    distribution: list[list[ProxyInfo]] = [
        [] for _ in range(worker_count)
    ]

    for i, proxy in enumerate(all_proxies):
        worker_idx = i % worker_count
        distribution[worker_idx].append(proxy)

    for worker_idx, proxies in enumerate(distribution):
        logger.debug(
            "proxies_distributed",
            worker_id=worker_idx,
            proxy_count=len(proxies),
            servers=[p.server for p in proxies],
        )

    return distribution


def _renumber_items(
    items: list[CatalogItemForWorker],
) -> list[CatalogItemForWorker]:
    """Перенумеровывает карточки для корректного отображения прогресса.

    При failover-раунде карточки сохраняют старые index/total.
    Эта функция обновляет нумерацию под новый размер списка.

    Args:
        items: Список карточек с устаревшей нумерацией.

    Returns:
        Тот же список с обновлёнными index и total.
    """
    total = len(items)
    for i, item in enumerate(items, start=1):
        item.index = i
        item.total = total
    return items


class ParallelListingService:
    """Сервис параллельной обработки карточек объявлений.

    Создаёт один процесс Chromium и N BrowserContext-ов (воркеров),
    каждый со своим прокси. Карточки распределяются через
    asyncio.Queue — воркер, освободившийся первым, берёт следующую.

    Все воркеры разделяют один ProxyHealthTracker. Когда прокси
    получает баны у любого воркера — трекер помечает его как DEAD,
    и все остальные воркеры автоматически пропускают его при ротации.
    Это устраняет бесполезные retry на заведомо мёртвых прокси.

    После основного раунда проваленные карточки автоматически
    перезапускаются в failover-раундах с использованием оставшихся
    здоровых прокси (до MAX_FAILOVER_ROUNDS попыток).

    При отсутствии прокси или max_workers=1 — работает в один
    поток, полностью совместим с текущим поведением.

    Attributes:
        _browser_settings: Настройки браузера.
        _proxy_settings: Настройки прокси и параллелизации.
        _repository: Репозиторий для сохранения результатов.
    """

    def __init__(
        self,
        browser_settings: BrowserSettings,
        proxy_settings: ProxySettings,
        repository: SQLiteListingRepository,
    ) -> None:
        """Инициализирует сервис параллельной обработки.

        Args:
            browser_settings: Настройки браузера (headless, таймауты).
            proxy_settings: Настройки прокси (файл, ротация, воркеры).
            repository: Репозиторий для сохранения объявлений.
        """
        self._browser_settings = browser_settings
        self._proxy_settings = proxy_settings
        self._repository = repository

    async def process_all(
        self,
        catalog_items: list[CatalogItemForWorker],
    ) -> list[RawListing]:
        """Обрабатывает все карточки объявлений параллельно.

        Основной публичный метод. Загружает прокси, создаёт общий
        ProxyHealthTracker, определяет количество воркеров, запускает
        общий Chromium, создаёт воркеров с общим трекером и ожидает
        завершения всех. После основного раунда запускает failover
        для проваленных карточек.

        Args:
            catalog_items: Список карточек из каталога для обработки.

        Returns:
            Список успешно спарсенных объявлений.
        """
        if not catalog_items:
            logger.info("parallel_processing_skipped_no_items")
            return []

        process_start = time.monotonic()

        # --- Загружаем прокси ---
        all_proxies = self._load_all_proxies()

        # --- Создаём общий трекер здоровья прокси ---
        shared_tracker = ProxyHealthTracker()
        shared_tracker.register_many(
            [p.server for p in all_proxies]
        )

        logger.info(
            "shared_proxy_health_tracker_created",
            total_proxies=shared_tracker.total_count,
        )

        # --- Определяем количество воркеров ---
        worker_count = _determine_worker_count(
            self._proxy_settings,
            len(all_proxies),
            len(catalog_items),
        )

        logger.info(
            "parallel_processing_started",
            total_items=len(catalog_items),
            worker_count=worker_count,
            proxy_count=len(all_proxies),
            max_workers_setting=self._proxy_settings.max_workers,
        )
        print(
            f"\n  [параллелизация] Запуск {worker_count} воркер(ов) "
            f"для обработки {len(catalog_items)} карточек"
        )

        # --- Запускаем общий Chromium ---
        playwright: Playwright | None = None
        browser: Browser | None = None
        all_listings: list[RawListing] = []
        all_worker_results: list[WorkerResult] = []
        worker_id_counter: int = 0

        try:
            playwright = await async_playwright().start()
            browser = await playwright.chromium.launch(
                headless=self._browser_settings.headless,
                args=BROWSER_ARGS,
            )

            logger.info(
                "shared_browser_launched",
                headless=self._browser_settings.headless,
                worker_count=worker_count,
            )

            # === ОСНОВНОЙ РАУНД ===
            round_results, worker_id_counter = await self._run_round(
                round_name="основной",
                round_number=0,
                items=catalog_items,
                all_proxies=all_proxies,
                shared_tracker=shared_tracker,
                browser=browser,
                all_listings=all_listings,
                worker_count=worker_count,
                worker_id_start=worker_id_counter,
            )
            all_worker_results.extend(round_results)

            # Собираем проваленные карточки
            failed_items = self._collect_failed_items(round_results)

            self._print_round_summary(
                round_name="Основной раунд",
                results=round_results,
                failed_remaining=len(failed_items),
            )

            # === FAILOVER-РАУНДЫ ===
            failover_round = 0

            while (
                failed_items
                and failover_round < MAX_FAILOVER_ROUNDS
            ):
                failover_round += 1

                # Проверяем наличие здоровых прокси
                healthy_proxies = [
                    p for p in all_proxies
                    if shared_tracker.is_healthy(p.server)
                ]

                if not healthy_proxies:
                    logger.warning(
                        "failover_no_healthy_proxies",
                        round=failover_round,
                        failed_count=len(failed_items),
                    )
                    print(
                        f"\n  ⚠️  Failover-раунд {failover_round}: "
                        f"нет здоровых прокси. "
                        f"Неспарсенных: {len(failed_items)}"
                    )
                    break

                # Перенумеровываем карточки для нового раунда
                _renumber_items(failed_items)

                # Количество воркеров для failover — не более
                # здоровых прокси и не более проваленных карточек
                failover_worker_count = min(
                    len(healthy_proxies),
                    len(failed_items),
                    MAX_AUTO_WORKERS,
                )
                failover_worker_count = max(failover_worker_count, 1)

                logger.info(
                    "failover_round_started",
                    round=failover_round,
                    failed_count=len(failed_items),
                    healthy_proxies=len(healthy_proxies),
                    worker_count=failover_worker_count,
                )
                print(
                    f"\n  🔄 Failover-раунд {failover_round}: "
                    f"{len(failed_items)} карточек → "
                    f"{failover_worker_count} воркер(ов) "
                    f"({len(healthy_proxies)} здоровых прокси)"
                )

                # Пауза перед failover — даём Avito «остыть»
                cooldown = random.uniform(
                    FAILOVER_COOLDOWN_MIN, FAILOVER_COOLDOWN_MAX
                )
                logger.info(
                    "failover_cooldown",
                    round=failover_round,
                    delay=round(cooldown, 1),
                )
                await asyncio.sleep(cooldown)

                # Запускаем failover-раунд
                round_results, worker_id_counter = (
                    await self._run_round(
                        round_name=f"failover_{failover_round}",
                        round_number=failover_round,
                        items=failed_items,
                        all_proxies=healthy_proxies,
                        shared_tracker=shared_tracker,
                        browser=browser,
                        all_listings=all_listings,
                        worker_count=failover_worker_count,
                        worker_id_start=worker_id_counter,
                    )
                )
                all_worker_results.extend(round_results)

                # Собираем оставшиеся проваленные
                failed_items = self._collect_failed_items(
                    round_results
                )

                self._print_round_summary(
                    round_name=f"Failover-раунд {failover_round}",
                    results=round_results,
                    failed_remaining=len(failed_items),
                )

            # === ФИНАЛЬНАЯ СТАТИСТИКА ===
            process_elapsed = time.monotonic() - process_start

            shared_tracker.log_summary()

            self._log_final_stats(
                catalog_items=catalog_items,
                all_worker_results=all_worker_results,
                failed_items=failed_items,
                shared_tracker=shared_tracker,
                worker_count=worker_count,
                failover_rounds=failover_round,
                total_elapsed=process_elapsed,
            )

        finally:
            # --- Закрываем общий браузер ---
            if browser is not None:
                try:
                    await browser.close()
                except Exception as e:
                    logger.warning(
                        "shared_browser_close_error",
                        error=str(e),
                    )

            if playwright is not None:
                try:
                    await playwright.stop()
                except Exception as e:
                    logger.warning(
                        "playwright_close_error",
                        error=str(e),
                    )

            logger.info("parallel_resources_closed")

        return all_listings

    async def _run_round(
        self,
        round_name: str,
        round_number: int,
        items: list[CatalogItemForWorker],
        all_proxies: list[ProxyInfo],
        shared_tracker: ProxyHealthTracker,
        browser: Browser,
        all_listings: list[RawListing],
        worker_count: int,
        worker_id_start: int,
    ) -> tuple[list[WorkerResult], int]:
        """Запускает один раунд параллельной обработки.

        Создаёт воркеров, распределяет прокси, наполняет очередь
        и ожидает завершения. Используется как для основного раунда,
        так и для failover-раундов.

        Args:
            round_name: Название раунда для логов.
            round_number: Номер раунда (0 = основной).
            items: Карточки для обработки.
            all_proxies: Доступные прокси для этого раунда.
            shared_tracker: Общий трекер здоровья прокси.
            browser: Общий Browser-инстанс.
            all_listings: Общий список результатов.
            worker_count: Количество воркеров.
            worker_id_start: Начальный ID для нумерации воркеров.

        Returns:
            Кортеж (список WorkerResult, следующий свободный worker_id).
        """
        # --- Распределяем прокси ---
        proxy_distribution = _distribute_proxies(
            all_proxies, worker_count
        )

        # --- Наполняем очередь ---
        queue: asyncio.Queue[CatalogItemForWorker | None] = (
            asyncio.Queue()
        )
        for item in items:
            await queue.put(item)

        for _ in range(worker_count):
            await queue.put(None)

        # --- Создаём и запускаем воркеров ---
        results_lock = asyncio.Lock()
        tasks: list[asyncio.Task[WorkerResult]] = []
        worker_browser_services: list[BrowserService] = []

        for i in range(worker_count):
            worker_id = worker_id_start + i
            worker_proxies = proxy_distribution[i]

            worker_browser_service = BrowserService(
                settings=self._browser_settings,
                proxy_settings=self._proxy_settings,
                assigned_proxies=worker_proxies,
                worker_id=worker_id,
                health_tracker=shared_tracker,
            )
            worker_browser_services.append(worker_browser_service)

            worker_listing_service = ListingService(
                browser_service=worker_browser_service,
            )

            task = asyncio.create_task(
                self._run_worker(
                    worker_id=worker_id,
                    browser=browser,
                    browser_service=worker_browser_service,
                    listing_service=worker_listing_service,
                    queue=queue,
                    all_listings=all_listings,
                    results_lock=results_lock,
                ),
                name=f"worker_{worker_id}",
            )
            tasks.append(task)

            # Стаггеринг между запуском воркеров
            if i < worker_count - 1:
                stagger = random.uniform(
                    WORKER_STAGGER_DELAY_MIN,
                    WORKER_STAGGER_DELAY_MAX,
                )
                logger.debug(
                    "worker_stagger_delay",
                    round=round_name,
                    next_worker_id=worker_id + 1,
                    delay=round(stagger, 1),
                )
                await asyncio.sleep(stagger)

        # --- Ожидаем завершения ---
        raw_results = await asyncio.gather(
            *tasks, return_exceptions=True
        )

        # --- Закрываем контексты воркеров этого раунда ---
        for ws in worker_browser_services:
            try:
                await ws.close()
            except Exception as e:
                logger.warning(
                    "worker_browser_close_error",
                    round=round_name,
                    error=str(e),
                )

        # --- Собираем результаты ---
        round_results: list[WorkerResult] = []

        for i, result in enumerate(raw_results):
            actual_worker_id = worker_id_start + i

            if isinstance(result, Exception):
                logger.error(
                    "worker_crashed",
                    round=round_name,
                    worker_id=actual_worker_id,
                    error=str(result),
                    error_type=type(result).__name__,
                )
                print(
                    f"  [воркер {actual_worker_id}] "
                    f"Аварийное завершение: {result}"
                )
            elif isinstance(result, WorkerResult):
                round_results.append(result)
                logger.info(
                    "worker_finished",
                    round=round_name,
                    worker_id=result.worker_id,
                    successful=result.successful,
                    failed=result.failed,
                    elapsed=round(result.elapsed, 1),
                )

        next_worker_id = worker_id_start + worker_count
        return round_results, next_worker_id

    async def _run_worker(
        self,
        worker_id: int,
        browser: Browser,
        browser_service: BrowserService,
        listing_service: ListingService,
        queue: asyncio.Queue[CatalogItemForWorker | None],
        all_listings: list[RawListing],
        results_lock: asyncio.Lock,
    ) -> WorkerResult:
        """Цикл работы одного воркера.

        Инициализирует свой BrowserContext, прогревает его,
        затем в цикле берёт карточки из очереди и парсит.
        Завершается при получении None (отравляющая пилюля).

        Args:
            worker_id: Идентификатор воркера.
            browser: Общий Browser-инстанс.
            browser_service: BrowserService воркера.
            listing_service: ListingService воркера.
            queue: Очередь карточек для обработки.
            all_listings: Общий список результатов (потокобезопасный
                через results_lock).
            results_lock: Лок для безопасного добавления в all_listings.

        Returns:
            Статистика работы воркера.
        """
        successful = 0
        failed = 0
        failed_items: list[CatalogItemForWorker] = []
        prefix = f"worker_{worker_id}"
        worker_start = time.monotonic()

        try:
            # --- Инициализация контекста ---
            page = await browser_service.launch_for_worker(browser)

            logger.info(
                "worker_started",
                worker_id=worker_id,
                proxy_count=len(browser_service._proxies),
            )

            # --- Прогрев: заходим на главную Avito ---
            await self._warmup_worker(browser_service, page, worker_id)

            # --- Основной цикл обработки карточек ---
            while True:
                item = await queue.get()

                # Отравляющая пилюля — завершаем воркер
                if item is None:
                    queue.task_done()
                    break

                logger.info(
                    "worker_processing_card",
                    worker_id=worker_id,
                    progress=f"{item.index}/{item.total}",
                    external_id=item.external_id,
                    title=item.title[:50],
                )
                print(
                    f"  [{prefix}] Карточка {item.index}/{item.total}: "
                    f"{item.external_id}"
                )

                try:
                    # Используем актуальную page из browser_service
                    current_page = browser_service.page or page

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
                        # Сохраняем в БД (потокобезопасно)
                        await self._repository.save_listing_async(
                            listing
                        )

                        # Добавляем в общий список результатов
                        async with results_lock:
                            all_listings.append(listing)

                        successful += 1
                        logger.info(
                            "worker_card_saved",
                            worker_id=worker_id,
                            progress=f"{item.index}/{item.total}",
                            external_id=listing.external_id,
                            room_category=listing.room_category.value,
                            avg_price=round(listing.average_price),
                        )
                    else:
                        failed += 1
                        failed_items.append(item)
                        logger.warning(
                            "worker_card_parse_failed",
                            worker_id=worker_id,
                            progress=f"{item.index}/{item.total}",
                            external_id=item.external_id,
                        )

                except Exception as e:
                    failed += 1
                    failed_items.append(item)
                    logger.error(
                        "worker_card_error",
                        worker_id=worker_id,
                        external_id=item.external_id,
                        error=str(e),
                        error_type=type(e).__name__,
                    )
                finally:
                    queue.task_done()

                # Обновляем page после возможной ротации внутри
                # listing_service
                if browser_service.page is not None:
                    page = browser_service.page

                # Проверяем плановую ротацию прокси по счётчику
                new_page = (
                    await browser_service.increment_and_check_rotation()
                )
                if new_page is not None:
                    page = new_page
                    logger.info(
                        "worker_proxy_rotated_by_counter",
                        worker_id=worker_id,
                        successful=successful,
                        failed=failed,
                    )
                    print(
                        f"  [{prefix}] Плановая смена прокси "
                        f"(обработано {successful + failed} карточек)"
                    )
                    # Прогрев после ротации
                    await self._warmup_worker(
                        browser_service, page, worker_id
                    )

                # Пауза между карточками — имитация человека
                delay = random.uniform(
                    WORKER_INTER_CARD_DELAY_MIN,
                    WORKER_INTER_CARD_DELAY_MAX,
                )
                await asyncio.sleep(delay)

        except Exception as e:
            logger.error(
                "worker_fatal_error",
                worker_id=worker_id,
                error=str(e),
                error_type=type(e).__name__,
                successful=successful,
                failed=failed,
            )
            print(
                f"  [{prefix}] Критическая ошибка: {e}"
            )

        worker_elapsed = time.monotonic() - worker_start

        logger.info(
            "worker_completed",
            worker_id=worker_id,
            successful=successful,
            failed=failed,
            elapsed=round(worker_elapsed, 1),
        )
        print(
            f"  [{prefix}] Завершён за {worker_elapsed:.1f} сек: "
            f"{successful} успешно, {failed} с ошибками"
        )

        return WorkerResult(
            worker_id=worker_id,
            successful=successful,
            failed=failed,
            elapsed=worker_elapsed,
            failed_items=failed_items,
        )

    async def _warmup_worker(
        self,
        browser_service: BrowserService,
        page: object,
        worker_id: int,
    ) -> None:
        """Прогревает контекст воркера.

        Заходит на главную Avito, чтобы получить cookies
        и выглядеть как обычный пользователь. Снижает
        вероятность бана на первой карточке.

        Args:
            browser_service: BrowserService воркера.
            page: Страница воркера (не используется напрямую,
                навигация идёт через browser_service).
            worker_id: ID воркера для логирования.
        """
        try:
            logger.info(
                "worker_warmup_started",
                worker_id=worker_id,
                url=WARMUP_URL,
            )

            success = await browser_service.navigate(WARMUP_URL)
            if success:
                await browser_service.simulate_human_behavior()
                logger.info(
                    "worker_warmup_completed",
                    worker_id=worker_id,
                )
            else:
                logger.warning(
                    "worker_warmup_failed",
                    worker_id=worker_id,
                )

        except Exception as e:
            logger.warning(
                "worker_warmup_error",
                worker_id=worker_id,
                error=str(e),
            )

    def _load_all_proxies(self) -> list[ProxyInfo]:
        """Загружает полный пул прокси из файла.

        Returns:
            Список всех прокси. Пустой список если путь не указан.
        """
        if not self._proxy_settings.proxy_file_path:
            logger.info("parallel_no_proxies_configured")
            return []

        try:
            proxies = load_proxies_from_file(
                self._proxy_settings.proxy_file_path
            )
            # Перемешиваем для равномерного распределения
            random.shuffle(proxies)
            return proxies
        except RuntimeError as e:
            logger.error(
                "parallel_proxy_load_failed",
                error=str(e),
            )
            return []

    @staticmethod
    def _collect_failed_items(
        results: list[WorkerResult],
    ) -> list[CatalogItemForWorker]:
        """Собирает проваленные карточки из результатов всех воркеров.

        Args:
            results: Список результатов воркеров.

        Returns:
            Объединённый список проваленных карточек.
        """
        failed: list[CatalogItemForWorker] = []
        for result in results:
            failed.extend(result.failed_items)
        return failed

    @staticmethod
    def _print_round_summary(
        round_name: str,
        results: list[WorkerResult],
        failed_remaining: int,
    ) -> None:
        """Печатает сводку по результатам раунда.

        Args:
            round_name: Название раунда для вывода.
            results: Результаты воркеров раунда.
            failed_remaining: Количество оставшихся проваленных.
        """
        total_ok = sum(r.successful for r in results)
        total_fail = sum(r.failed for r in results)
        status = "✅" if total_fail == 0 else "⚠️ "

        print(
            f"\n  {status} {round_name}: "
            f"✅ {total_ok} успешно | ❌ {total_fail} провалено"
        )

        if failed_remaining > 0:
            print(
                f"     Осталось для failover: {failed_remaining}"
            )

    def _log_final_stats(
        self,
        catalog_items: list[CatalogItemForWorker],
        all_worker_results: list[WorkerResult],
        failed_items: list[CatalogItemForWorker],
        shared_tracker: ProxyHealthTracker,
        worker_count: int,
        failover_rounds: int,
        total_elapsed: float,
    ) -> None:
        """Логирует и выводит финальную статистику.

        Выводит подробный отчёт: общее время, время по воркерам,
        среднее на карточку, количество failover-раундов,
        состояние прокси.

        Args:
            catalog_items: Исходный список карточек.
            all_worker_results: Результаты всех воркеров (вкл. failover).
            failed_items: Оставшиеся необработанные карточки.
            shared_tracker: Трекер здоровья прокси.
            worker_count: Количество воркеров основного раунда.
            failover_rounds: Количество failover-раундов.
            total_elapsed: Общее время обработки в секундах.
        """
        total_successful = sum(
            r.successful for r in all_worker_results
        )
        total_failed = len(failed_items)

        # --- Статистика по воркерам ---
        worker_timings: dict[str, float] = {}
        for wr in all_worker_results:
            worker_timings[f"worker_{wr.worker_id}"] = round(
                wr.elapsed, 1
            )

        # Среднее время на карточку
        total_cards_processed = sum(
            r.successful + r.failed for r in all_worker_results
        )
        avg_per_card = (
            total_elapsed / total_cards_processed
            if total_cards_processed > 0
            else 0.0
        )

        # Логируем в JSON
        logger.info(
            "parallel_processing_completed",
            total_items=len(catalog_items),
            total_successful=total_successful,
            total_failed=total_failed,
            worker_count=worker_count,
            failover_rounds=failover_rounds,
            total_elapsed=round(total_elapsed, 1),
            avg_per_card=round(avg_per_card, 1),
            proxies_alive=shared_tracker.alive_count,
            proxies_dead=(
                shared_tracker.total_count
                - shared_tracker.alive_count
            ),
            worker_timings=worker_timings,
        )

        # --- Консольный вывод ---
        print(
            f"\n  {'═' * 61}"
            f"\n  ИТОГИ ПАРАЛЛЕЛЬНОЙ ОБРАБОТКИ"
            f"\n  {'═' * 61}"
            f"\n  Всего карточек:            "
            f"{len(catalog_items)}"
            f"\n  Успешно спарсено:          "
            f"{total_successful}"
            f"\n  Не удалось спарсить:       "
            f"{total_failed}"
            f"\n  Воркеров (основной раунд): "
            f"{worker_count}"
            f"\n  Failover-раундов:          "
            f"{failover_rounds}"
            f"\n  Общее время:               "
            f"{total_elapsed:.1f} сек "
            f"({total_elapsed / 60:.1f} мин)"
            f"\n  Среднее на карточку:       "
            f"{avg_per_card:.1f} сек"
            f"\n  Прокси живых/всего:        "
            f"{shared_tracker.alive_count}/"
            f"{shared_tracker.total_count}"
        )

        # Таблица воркеров
        if all_worker_results:
            print(f"\n  {'─' * 61}")
            print("  ВРЕМЯ ВЫПОЛНЕНИЯ ВОРКЕРОВ")
            print(f"  {'─' * 61}")

            sorted_results = sorted(
                all_worker_results, key=lambda r: r.worker_id
            )

            for wr in sorted_results:
                cards_total = wr.successful + wr.failed
                wr_avg = (
                    wr.elapsed / cards_total
                    if cards_total > 0
                    else 0.0
                )
                status_icon = "✅" if not wr.failed_items else "⚠️ "

                print(
                    f"    {status_icon} worker_{wr.worker_id:>2}: "
                    f"{wr.elapsed:>7.1f} сек "
                    f"| {wr.successful:>3} ✅ "
                    f"{wr.failed:>3} ❌ "
                    f"| ~{wr_avg:.1f} сек/карточка"
                )

            # Сводная статистика по времени воркеров
            elapsed_list = [
                wr.elapsed for wr in all_worker_results
            ]
            if elapsed_list:
                print(f"\n  {'─' * 61}")
                print(
                    f"  Мин. время воркера:        "
                    f"{min(elapsed_list):.1f} сек"
                )
                print(
                    f"  Макс. время воркера:       "
                    f"{max(elapsed_list):.1f} сек"
                )
                print(
                    f"  Разброс:                   "
                    f"{max(elapsed_list) - min(elapsed_list):.1f} сек"
                )

        # Неспарсенные карточки
        if failed_items:
            failed_ids = [item.external_id for item in failed_items]
            print(
                f"\n  ⚠️  Неспарсенные карточки "
                f"({len(failed_items)}): "
                f"{', '.join(failed_ids[:20])}"
            )
            if len(failed_ids) > 20:
                print(
                    f"     ... и ещё "
                    f"{len(failed_ids) - 20}"
                )

        print(f"  {'═' * 61}\n")
