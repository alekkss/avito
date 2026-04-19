"""Сервис парсинга объявлений из каталога Avito.

Извлекает базовые данные объявлений из HTML-страниц каталога Avito,
используя CSS-селекторы на основе data-marker атрибутов.
Поддерживает обход пагинации с retry-логикой, обнаружение
циклов и детальный парсинг карточек через ListingService.

Публичный интерфейс:
- scrape_catalog() — обход каталога, возврат списка CatalogItem.
- scrape_all() — полный цикл: каталог + последовательный парсинг
  карточек (для обратной совместимости и тестов).
"""

import asyncio
import random
import re
from dataclasses import dataclass
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from playwright.async_api import Page

from src.config import ScraperSettings, get_logger
from src.models import RawListing
from src.repositories.base import BaseListingRepository
from src.services.browser_service import BrowserService
from src.services.listing_service import ListingService

logger = get_logger("scraper_service")

# Количество попыток дождаться элемента на странице
MAX_ELEMENT_RETRIES: int = 10
# Ожидание между попытками загрузки (секунды)
ELEMENT_RETRY_WAIT: int = 15
# Максимальное количество пустых страниц подряд перед остановкой
MAX_EMPTY_PAGES: int = 2
# Порог дубликатов товаров на странице для обнаружения цикла (%)
DUPLICATE_THRESHOLD: float = 0.8
# Максимальное количество попыток перехода на следующую страницу
MAX_PAGINATION_RETRIES: int = 3
# Количество попыток ожидания разблокировки
MAX_UNBLOCK_RETRIES: int = 10
# Ожидание между проверками разблокировки (секунды)
UNBLOCK_WAIT: int = 15
# Максимальное количество попыток первоначальной навигации
MAX_INITIAL_NAVIGATION_RETRIES: int = 5
# Таймаут ожидания контейнера после пагинации (мс)
# Короткий таймаут: если контейнера нет — товары закончились
PAGINATION_CONTAINER_TIMEOUT: int = 15000

# Тексты бейджа мгновенного бронирования в каталоге
INSTANT_BOOK_BADGES: tuple[str, ...] = (
    "мгновенная бронь",
    "мгновенное бронирование",
    "моментальная бронь",
)

# CSS-селектор рейтинга хоста в карточке каталога
SELLER_SCORE_SELECTOR: str = "[data-marker='seller-info/score']"

# URL для «прогрева» нового контекста после ротации прокси
WARMUP_URL: str = "https://www.avito.ru"


@dataclass
class CatalogItem:
    """Промежуточные данные объявления из каталога.

    Содержит базовую информацию, извлечённую из списка каталога.
    Используется как входные данные для ListingService,
    который парсит детальную карточку объявления.

    Attributes:
        avito_id: Уникальный идентификатор объявления на Avito.
        title: Название объявления из каталога.
        price: Базовая цена из каталога (руб./сут.).
        url: Относительная ссылка на страницу объявления.
        is_instant_book: Наличие бейджа «Мгновенная бронь» в каталоге.
        host_rating: Рейтинг хоста из каталога (0.0 если не найден).
    """

    avito_id: str
    title: str
    price: int
    url: str
    is_instant_book: bool = False
    host_rating: float = 0.0

    @property
    def external_id(self) -> str:
        """Формирует external_id в формате "av_<id>" для склейки данных.

        Returns:
            Идентификатор в формате "av_<avito_id>".
        """
        return f"av_{self.avito_id}"


class ScraperService:
    """Сервис для парсинга объявлений из каталога Avito.

    Координирует работу BrowserService для навигации, извлекает
    базовые данные из каталога и запускает детальный парсинг
    карточек через ListingService. Поддерживает пагинацию,
    обнаружение циклов, ротацию прокси и батчевое сохранение.

    Публичный интерфейс:
    - scrape_catalog(): обход каталога → список CatalogItem.
    - scrape_all(): полный цикл (каталог + карточки) для обратной
      совместимости и тестовых скриптов.

    Attributes:
        _browser_service: Сервис управления браузером.
        _listing_service: Сервис парсинга карточек объявлений.
        _repository: Репозиторий для сохранения объявлений.
        _settings: Настройки парсера (URL категории, лимит страниц).
        _seen_avito_ids: Множество уже встреченных ID объявлений.
        _total_pages: Общее количество страниц (определяется из пагинации).
        _base_url: Базовый URL категории (без параметра p).
    """

    # CSS-селекторы для элементов каталога Avito
    CATALOG_CONTAINER = "div#bx_serp-item-list"
    ITEM_CARD = "div[data-marker='item']"
    ITEM_TITLE = "a[data-marker='item-title']"
    ITEM_PRICE_META = "meta[itemprop='price']"

    def __init__(
        self,
        browser_service: BrowserService,
        listing_service: ListingService,
        repository: BaseListingRepository,
        settings: ScraperSettings,
    ) -> None:
        """Инициализирует сервис парсинга.

        Args:
            browser_service: Сервис для управления браузером.
            listing_service: Сервис для парсинга карточек объявлений.
            repository: Репозиторий для сохранения объявлений.
            settings: Настройки парсера (URL, лимит страниц).
        """
        self._browser_service = browser_service
        self._listing_service = listing_service
        self._repository = repository
        self._settings = settings
        self._seen_avito_ids: set[str] = set()
        self._total_pages: int = 0
        self._base_url: str = ""

    def _extract_page_number_from_url(self, url: str) -> int:
        """Извлекает номер страницы из параметра p в URL.

        Args:
            url: URL страницы.

        Returns:
            Номер страницы или 1, если параметр отсутствует.
        """
        try:
            parsed = urlparse(url)
            params = parse_qs(parsed.query)
            page_str = params.get("p", ["1"])[0]
            return int(page_str)
        except (ValueError, IndexError):
            return 1

    def _capture_base_url(self, url: str) -> None:
        """Запоминает базовый URL категории после первой загрузки.

        Args:
            url: Реальный URL из адресной строки браузера.
        """
        parsed = urlparse(url)
        params = parse_qs(parsed.query, keep_blank_values=True)

        params.pop("p", None)

        clean_query = urlencode(params, doseq=True)
        self._base_url = urlunparse((
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            clean_query,
            "",
        ))

        logger.info(
            "base_url_captured",
            base_url=self._base_url[:200],
        )

    def _build_page_url(self, page_num: int) -> str:
        """Конструирует URL для конкретной страницы пагинации.

        Args:
            page_num: Номер страницы.

        Returns:
            Полный URL страницы.
        """
        if not self._base_url:
            logger.error("base_url_not_captured")
            return ""

        if page_num <= 1:
            return self._base_url

        separator = "&" if "?" in self._base_url else "?"
        return f"{self._base_url}{separator}p={page_num}"

    async def _detect_total_pages(self, page: Page) -> int:
        """Определяет общее количество страниц из пагинации.

        Args:
            page: Активная страница Playwright.

        Returns:
            Номер последней страницы или 0 если пагинация не найдена.
        """
        try:
            all_buttons = await page.query_selector_all(
                "[data-marker^='pagination-button/page(']"
            )

            if not all_buttons:
                logger.debug("no_pagination_buttons_found")
                return 0

            max_page = 0
            for button in all_buttons:
                marker = await button.get_attribute("data-marker")
                if marker:
                    try:
                        start = marker.index("(") + 1
                        end = marker.index(")")
                        num = int(marker[start:end])
                        if num > max_page:
                            max_page = num
                    except (ValueError, IndexError):
                        continue

            if max_page > 0:
                logger.info(
                    "total_pages_detected",
                    total_pages=max_page,
                )

            return max_page

        except Exception as e:
            logger.warning(
                "total_pages_detection_failed",
                error=str(e),
            )
            return 0

    async def _wait_for_unblock(self, page: Page, context: str) -> bool:
        """Ожидает снятия блокировки Avito с повторными попытками.

        Args:
            page: Активная страница Playwright.
            context: Контекст вызова для логирования.

        Returns:
            True если блокировка снята.
        """
        for attempt in range(1, MAX_UNBLOCK_RETRIES + 1):
            logger.warning(
                "block_detected_waiting",
                context=context,
                attempt=attempt,
                max_attempts=MAX_UNBLOCK_RETRIES,
                wait_seconds=UNBLOCK_WAIT,
            )

            await asyncio.sleep(UNBLOCK_WAIT)

            try:
                current_url = page.url
                await page.reload(wait_until="domcontentloaded")
                logger.info(
                    "page_reloaded_after_block",
                    context=context,
                    attempt=attempt,
                    url=current_url[:200],
                )
            except Exception as e:
                logger.warning(
                    "page_reload_failed_after_block",
                    context=context,
                    attempt=attempt,
                    error=str(e),
                )
                continue

            await asyncio.sleep(5)

            is_blocked = await self._browser_service._check_blocked()
            if not is_blocked:
                logger.info(
                    "block_resolved",
                    context=context,
                    attempt=attempt,
                )
                return True

            logger.warning(
                "still_blocked",
                context=context,
                attempt=attempt,
            )

        logger.error(
            "block_not_resolved",
            context=context,
            max_attempts=MAX_UNBLOCK_RETRIES,
            total_wait_seconds=MAX_UNBLOCK_RETRIES * UNBLOCK_WAIT,
        )
        return False

    async def _initial_navigate_with_retry(self, page: Page) -> bool:
        """Выполняет первоначальную навигацию с ожиданием разблокировки.

        Args:
            page: Активная страница Playwright.

        Returns:
            True если навигация успешна.
        """
        url = self._settings.category_url

        success = await self._browser_service.navigate(url)
        if success:
            return True

        is_blocked = await self._browser_service._check_blocked()
        if not is_blocked:
            logger.error(
                "initial_navigation_failed_not_blocked",
                url=url,
            )
            return False

        logger.warning(
            "initial_navigation_blocked",
            url=url,
        )

        unblocked = await self._wait_for_unblock(
            page,
            context="initial_navigation",
        )
        if not unblocked:
            logger.error(
                "initial_navigation_block_not_resolved",
                url=url,
                max_attempts=MAX_UNBLOCK_RETRIES,
            )
            return False

        for attempt in range(1, MAX_INITIAL_NAVIGATION_RETRIES + 1):
            logger.info(
                "initial_navigation_retry",
                attempt=attempt,
                max_attempts=MAX_INITIAL_NAVIGATION_RETRIES,
                url=url,
            )

            success = await self._browser_service.navigate(url)
            if success:
                logger.info(
                    "initial_navigation_retry_success",
                    attempt=attempt,
                )
                return True

            is_blocked = await self._browser_service._check_blocked()
            if is_blocked:
                unblocked = await self._wait_for_unblock(
                    page,
                    context=f"initial_navigation_retry:{attempt}",
                )
                if not unblocked:
                    logger.error(
                        "initial_navigation_permanently_blocked",
                        attempt=attempt,
                    )
                    return False
            else:
                logger.error(
                    "initial_navigation_retry_failed",
                    attempt=attempt,
                    url=url,
                )
                if attempt < MAX_INITIAL_NAVIGATION_RETRIES:
                    await asyncio.sleep(ELEMENT_RETRY_WAIT)
                    continue
                return False

        logger.error(
            "initial_navigation_all_retries_exhausted",
            max_attempts=MAX_INITIAL_NAVIGATION_RETRIES,
            url=url,
        )
        return False

    async def _check_container_after_pagination(
        self, page: Page, target_page_num: int
    ) -> bool:
        """Проверяет наличие контейнера с объявлениями после пагинации.

        Args:
            page: Активная страница Playwright.
            target_page_num: Номер целевой страницы (для логов).

        Returns:
            True если контейнер с объявлениями найден.
        """
        try:
            await page.wait_for_selector(
                self.CATALOG_CONTAINER,
                timeout=PAGINATION_CONTAINER_TIMEOUT,
            )
            logger.info(
                "pagination_container_found",
                target_page=target_page_num,
                container=self.CATALOG_CONTAINER,
            )
            return True
        except Exception:
            pass

        is_blocked = await self._browser_service._check_blocked()
        if is_blocked:
            logger.warning(
                "pagination_page_blocked",
                target_page=target_page_num,
            )
            unblocked = await self._wait_for_unblock(
                page,
                context=f"pagination_container:page_{target_page_num}",
            )
            if unblocked:
                try:
                    await page.wait_for_selector(
                        self.CATALOG_CONTAINER,
                        timeout=PAGINATION_CONTAINER_TIMEOUT,
                    )
                    logger.info(
                        "pagination_container_found_after_unblock",
                        target_page=target_page_num,
                    )
                    return True
                except Exception:
                    pass

            return False

        logger.info(
            "pagination_no_more_items",
            target_page=target_page_num,
            container=self.CATALOG_CONTAINER,
        )
        return False

    async def _warmup_after_rotation(self, page: Page) -> None:
        """Прогревает новый контекст после ротации прокси.

        Заходит на главную страницу Avito, чтобы новый контекст
        получил cookies и выглядел как обычный пользователь.
        Это снижает вероятность бана на первой же карточке.

        Args:
            page: Новая страница после ротации.
        """
        try:
            logger.info(
                "warmup_started",
                url=WARMUP_URL,
            )

            await page.goto(
                WARMUP_URL,
                wait_until="domcontentloaded",
                timeout=30000,
            )

            # Имитируем короткое пребывание на главной
            await asyncio.sleep(random.uniform(2.0, 4.0))
            await self._browser_service.simulate_human_behavior()

            logger.info("warmup_completed")

        except Exception as e:
            logger.warning(
                "warmup_failed",
                error=str(e),
            )

    async def scrape_catalog(self) -> list[CatalogItem]:
        """Обходит каталог Avito и возвращает список базовых данных.

        Публичный метод для использования в __main__.py.
        Выполняет полный цикл: запуск браузера, навигация,
        обход пагинации, сбор CatalogItem.

        Браузер НЕ закрывается после завершения — это позволяет
        вызывающему коду переиспользовать его или закрыть вручную.

        Returns:
            Список CatalogItem с базовыми данными из каталога.
        """
        self._seen_avito_ids.clear()
        self._total_pages = 0
        self._base_url = ""

        page = await self._browser_service.launch()

        success = await self._initial_navigate_with_retry(page)
        if not success:
            logger.error(
                "initial_navigation_failed",
                url=self._settings.category_url,
            )
            return []

        await self._browser_service.simulate_human_behavior()

        self._capture_base_url(page.url)
        self._total_pages = await self._detect_total_pages(page)

        logger.info("catalog_scraping_started")
        all_catalog_items = await self._collect_catalog_pages(page)

        if not all_catalog_items:
            logger.warning("no_catalog_items_found")
            return []

        logger.info(
            "catalog_scraping_completed",
            total_items=len(all_catalog_items),
        )

        return all_catalog_items

    async def scrape_all(self) -> list[RawListing]:
        """Парсит все страницы каталога и карточки объявлений.

        Полный цикл для обратной совместимости: каталог +
        последовательный парсинг карточек. Используется
        в тестовом скрипте test_single_listing.py.

        Для параллельной обработки используйте scrape_catalog()
        + ParallelListingService.process_all() в __main__.py.

        Returns:
            Полный список спарсенных объявлений аренды.
        """
        all_catalog_items = await self.scrape_catalog()

        if not all_catalog_items:
            return []

        page = self._browser_service.page
        if page is None:
            logger.error("no_active_page_after_catalog")
            return []

        # === Детальный парсинг карточек (последовательный) ===
        logger.info(
            "detail_parsing_started",
            total_items=len(all_catalog_items),
        )
        all_listings = await self._parse_all_listings(
            page, all_catalog_items
        )

        logger.info(
            "scraping_completed",
            catalog_items=len(all_catalog_items),
            parsed_listings=len(all_listings),
        )

        return all_listings

    async def _collect_catalog_pages(
        self, page: Page
    ) -> list[CatalogItem]:
        """Обходит все страницы каталога и собирает базовые данные.

        Args:
            page: Активная страница Playwright.

        Returns:
            Список промежуточных данных из каталога.
        """
        all_items: list[CatalogItem] = []
        current_page_num = 1
        max_pages = self._settings.max_pages
        consecutive_empty_pages = 0

        while True:
            logger.info(
                "scraping_page",
                page_number=current_page_num,
                url_page_number=self._extract_page_number_from_url(
                    page.url
                ),
                max_pages=max_pages if max_pages > 0 else "unlimited",
                total_pages=(
                    self._total_pages if self._total_pages > 0
                    else "unknown"
                ),
                total_unique_items=len(self._seen_avito_ids),
            )

            items = await self._parse_current_page(page)

            if items:
                new_items: list[CatalogItem] = []
                duplicate_count = 0

                for item in items:
                    if item.avito_id in self._seen_avito_ids:
                        duplicate_count += 1
                    else:
                        self._seen_avito_ids.add(item.avito_id)
                        new_items.append(item)

                if len(items) > 0:
                    duplicate_ratio = duplicate_count / len(items)
                    if duplicate_ratio >= DUPLICATE_THRESHOLD:
                        logger.warning(
                            "pagination_cycle_detected",
                            page_number=current_page_num,
                            total_items=len(items),
                            duplicates=duplicate_count,
                            duplicate_ratio=f"{duplicate_ratio:.0%}",
                        )
                        all_items.extend(new_items)
                        break

                all_items.extend(new_items)
                consecutive_empty_pages = 0

                logger.info(
                    "page_scraped",
                    page_number=current_page_num,
                    items_on_page=len(items),
                    new_items=len(new_items),
                    duplicates=duplicate_count,
                    total_items=len(all_items),
                )
            else:
                consecutive_empty_pages += 1
                logger.warning(
                    "no_items_on_page",
                    page_number=current_page_num,
                    consecutive_empty=consecutive_empty_pages,
                )

                if consecutive_empty_pages >= MAX_EMPTY_PAGES:
                    logger.warning(
                        "too_many_empty_pages",
                        consecutive_empty=consecutive_empty_pages,
                        max_allowed=MAX_EMPTY_PAGES,
                    )
                    break

            if 0 < max_pages <= current_page_num:
                logger.info(
                    "max_pages_reached",
                    max_pages=max_pages,
                )
                break

            if (
                self._total_pages > 0
                and current_page_num >= self._total_pages
            ):
                logger.info(
                    "last_page_reached",
                    current_page=current_page_num,
                    total_pages=self._total_pages,
                )
                break

            has_next = await self._go_to_next_page(
                page, current_page_num
            )
            if not has_next:
                logger.info(
                    "pagination_ended",
                    last_page=current_page_num,
                )
                break

            current_page_num += 1

            updated_total = await self._detect_total_pages(page)
            if updated_total > 0:
                self._total_pages = updated_total

            await self._browser_service.simulate_human_behavior()

        return all_items

    async def _parse_all_listings(
        self,
        page: Page,
        catalog_items: list[CatalogItem],
    ) -> list[RawListing]:
        """Парсит детальные карточки всех объявлений.

        После каждой успешно обработанной карточки проверяет
        необходимость ротации прокси по счётчику. При ротации
        прогревает новый контекст и продолжает обход.

        Args:
            page: Активная страница Playwright.
            catalog_items: Список базовых данных из каталога.

        Returns:
            Список полностью спарсенных объявлений.
        """
        all_listings: list[RawListing] = []
        total = len(catalog_items)

        # Используем текущую page, которая может обновиться при ротации
        current_page = page

        for index, item in enumerate(catalog_items, start=1):
            logger.info(
                "parsing_listing",
                progress=f"{index}/{total}",
                external_id=item.external_id,
                title=item.title[:50],
                is_instant_book=item.is_instant_book,
                host_rating=item.host_rating,
            )

            listing = await self._listing_service.parse_listing(
                page=current_page,
                external_id=item.external_id,
                url=item.url,
                title=item.title,
                base_price=item.price,
                is_instant_book=item.is_instant_book,
                catalog_host_rating=item.host_rating,
            )

            if listing is not None:
                self._repository.save_listing(listing)
                all_listings.append(listing)

                logger.info(
                    "listing_saved",
                    progress=f"{index}/{total}",
                    external_id=listing.external_id,
                    room_category=listing.room_category.value,
                    is_instant_book=listing.is_instant_book,
                    host_rating=listing.host_rating,
                )
            else:
                logger.warning(
                    "listing_parse_failed",
                    progress=f"{index}/{total}",
                    external_id=item.external_id,
                    url=item.url[:100],
                )

            # После каждой карточки — обновляем current_page
            # (мог измениться после ротации внутри listing_service)
            if self._browser_service.page is not None:
                current_page = self._browser_service.page

            # Проверяем необходимость плановой ротации прокси по счётчику
            new_page = await self._browser_service.increment_and_check_rotation()
            if new_page is not None:
                logger.info(
                    "proxy_rotated_by_counter",
                    progress=f"{index}/{total}",
                    listings_processed=index,
                )
                print(
                    f"\n  [прокси] Плановая смена прокси после "
                    f"{index} карточек"
                )

                current_page = new_page

                # Прогреваем новый контекст
                await self._warmup_after_rotation(current_page)

            if index % 10 == 0:
                logger.info(
                    "detail_parsing_progress",
                    parsed=index,
                    total=total,
                    successful=len(all_listings),
                    failed=index - len(all_listings),
                )

        return all_listings

    async def _scroll_page_naturally(self, page: Page) -> None:
        """Прокручивает страницу как реальный пользователь.

        Args:
            page: Активная страница Playwright.
        """
        try:
            total_height = await page.evaluate(
                "document.body.scrollHeight"
            )
            viewport_height = await page.evaluate("window.innerHeight")

            if total_height <= viewport_height:
                logger.debug("page_too_short_to_scroll")
                return

            current_position = 0
            scroll_step_min = 200
            scroll_step_max = 500

            while current_position < total_height:
                scroll_amount = random.randint(
                    scroll_step_min, scroll_step_max
                )
                current_position += scroll_amount

                if current_position > total_height:
                    current_position = total_height

                await page.evaluate(
                    f"window.scrollTo(0, {current_position})"
                )

                pause = random.uniform(0.3, 1.2)
                await asyncio.sleep(pause)

                if random.random() < 0.15:
                    back_scroll = random.randint(50, 150)
                    current_position = max(
                        0, current_position - back_scroll
                    )
                    await page.evaluate(
                        f"window.scrollTo(0, {current_position})"
                    )
                    await asyncio.sleep(random.uniform(0.3, 0.7))

            await asyncio.sleep(random.uniform(0.5, 1.5))
            await page.evaluate("window.scrollTo(0, 0)")
            await asyncio.sleep(random.uniform(0.5, 1.0))

            logger.debug("scroll_completed")

        except Exception as e:
            logger.warning(
                "scroll_failed",
                error=str(e),
            )

    async def _wait_for_element_with_retry(
        self,
        page: Page,
        selector: str,
        element_name: str,
    ) -> bool:
        """Ожидает появления элемента на странице с повторными попытками.

        Args:
            page: Активная страница Playwright.
            selector: CSS-селектор искомого элемента.
            element_name: Человекочитаемое имя элемента для логов.

        Returns:
            True если элемент появился на странице.
        """
        for attempt in range(1, MAX_ELEMENT_RETRIES + 1):
            try:
                await page.wait_for_selector(
                    selector,
                    timeout=15000,
                )
                logger.info(
                    "element_found",
                    element=element_name,
                    selector=selector,
                    attempt=attempt,
                )
                return True
            except Exception:
                pass

            is_blocked = await self._browser_service._check_blocked()
            if is_blocked:
                unblocked = await self._wait_for_unblock(
                    page,
                    context=f"element_wait:{element_name}",
                )
                if unblocked:
                    continue
                return False

            if attempt >= MAX_ELEMENT_RETRIES:
                logger.error(
                    "element_retry_exhausted",
                    element=element_name,
                    selector=selector,
                    attempts=MAX_ELEMENT_RETRIES,
                    url=page.url,
                )
                return False

            logger.warning(
                "element_not_found_retrying",
                element=element_name,
                attempt=attempt,
                max_attempts=MAX_ELEMENT_RETRIES,
                wait_seconds=ELEMENT_RETRY_WAIT,
                url=page.url,
            )

            try:
                current_url = page.url
                await page.reload(wait_until="domcontentloaded")
                logger.info(
                    "page_reloaded_for_element",
                    element=element_name,
                    attempt=attempt,
                    url=current_url,
                )
            except Exception as e:
                logger.warning(
                    "page_reload_failed",
                    element=element_name,
                    attempt=attempt,
                    error=str(e),
                )

            await asyncio.sleep(ELEMENT_RETRY_WAIT)

        return False

    async def _parse_current_page(
        self, page: Page
    ) -> list[CatalogItem]:
        """Парсит карточки объявлений на текущей странице каталога.

        Args:
            page: Активная страница Playwright.

        Returns:
            Список промежуточных данных из каталога.
        """
        items: list[CatalogItem] = []

        catalog_found = await self._wait_for_element_with_retry(
            page,
            self.CATALOG_CONTAINER,
            "catalog_container",
        )
        if not catalog_found:
            logger.warning("catalog_container_not_found_after_retries")
            return items

        await self._scroll_page_naturally(page)

        container = await page.query_selector(self.CATALOG_CONTAINER)
        if not container:
            logger.warning(
                "catalog_container_disappeared_after_scroll",
                selector=self.CATALOG_CONTAINER,
            )
            return items

        item_cards = await container.query_selector_all(self.ITEM_CARD)

        if not item_cards:
            logger.warning(
                "no_item_cards_in_container",
                container=self.CATALOG_CONTAINER,
            )
            return items

        logger.info(
            "item_cards_found",
            count=len(item_cards),
            container=self.CATALOG_CONTAINER,
        )

        for card in item_cards:
            try:
                item = await self._parse_single_card(card)
                if item is not None:
                    items.append(item)
            except Exception as e:
                logger.warning(
                    "card_parse_error",
                    error=str(e),
                    error_type=type(e).__name__,
                )

        return items

    async def _parse_single_card(
        self, card: object
    ) -> CatalogItem | None:
        """Извлекает базовые данные объявления из карточки каталога.

        Парсит avito_id, title, price, url, наличие бейджа
        «Мгновенная бронь» и рейтинг хоста.

        Args:
            card: ElementHandle карточки объявления.

        Returns:
            CatalogItem с базовыми данными или None.
        """
        avito_id = await card.get_attribute("data-item-id")
        if not avito_id:
            logger.debug("card_missing_avito_id")
            return None

        title = ""
        url = ""
        title_element = await card.query_selector(self.ITEM_TITLE)
        if title_element:
            title = (await title_element.inner_text()).strip()
            url = await title_element.get_attribute("href") or ""

        if not title:
            logger.debug("card_missing_title", avito_id=avito_id)
            return None

        price = 0
        price_meta = await card.query_selector(self.ITEM_PRICE_META)
        if price_meta:
            price_str = await price_meta.get_attribute("content") or "0"
            try:
                price = int(price_str)
            except ValueError:
                logger.debug(
                    "card_invalid_price",
                    avito_id=avito_id,
                    price_str=price_str,
                )

        # Проверяем наличие бейджа «Мгновенная бронь» в карточке
        is_instant_book = False
        try:
            card_text = (await card.inner_text()).lower()
            for badge_text in INSTANT_BOOK_BADGES:
                if badge_text in card_text:
                    is_instant_book = True
                    break
        except Exception as e:
            logger.debug(
                "instant_book_badge_check_failed",
                avito_id=avito_id,
                error=str(e),
            )

        # Извлекаем рейтинг хоста из карточки каталога
        host_rating = 0.0
        try:
            score_el = await card.query_selector(SELLER_SCORE_SELECTOR)
            if score_el:
                score_text = (await score_el.inner_text()).strip()
                # Заменяем запятую на точку: "4,4" → "4.4"
                cleaned = score_text.replace(",", ".").strip()
                match = re.search(r"(\d+\.?\d*)", cleaned)
                if match:
                    rating_val = float(match.group(1))
                    if 0.0 <= rating_val <= 5.0:
                        host_rating = rating_val
        except Exception as e:
            logger.debug(
                "host_rating_extraction_failed",
                avito_id=avito_id,
                error=str(e),
            )

        item = CatalogItem(
            avito_id=avito_id,
            title=title,
            price=price,
            url=url,
            is_instant_book=is_instant_book,
            host_rating=host_rating,
        )

        logger.debug(
            "catalog_item_parsed",
            avito_id=avito_id,
            title=title[:50],
            price=price,
            is_instant_book=is_instant_book,
            host_rating=host_rating,
        )

        return item

    async def _go_to_next_page(
        self, page: Page, current_page_num: int
    ) -> bool:
        """Переходит на следующую страницу пагинации.

        Args:
            page: Активная страница Playwright.
            current_page_num: Номер текущей страницы.

        Returns:
            True если переход успешен.
        """
        target_page_num = current_page_num + 1

        next_url = self._build_page_url(target_page_num)
        if not next_url:
            logger.error(
                "cannot_build_next_page_url",
                target_page=target_page_num,
            )
            return False

        logger.info(
            "next_page_url_constructed",
            target_page=target_page_num,
            next_url=next_url[:200],
        )

        for attempt in range(1, MAX_PAGINATION_RETRIES + 1):
            try:
                delay = random.uniform(2.0, 5.0)
                logger.info(
                    "next_page_navigating",
                    target_page=target_page_num,
                    attempt=attempt,
                    delay=round(delay, 1),
                )
                await asyncio.sleep(delay)

                await page.goto(
                    next_url,
                    wait_until="domcontentloaded",
                    timeout=60000,
                )

                logger.info(
                    "next_page_waiting",
                    wait_seconds=ELEMENT_RETRY_WAIT,
                )
                await asyncio.sleep(ELEMENT_RETRY_WAIT)

                is_blocked = await self._browser_service._check_blocked()
                if is_blocked:
                    unblocked = await self._wait_for_unblock(
                        page,
                        context=f"pagination:page_{target_page_num}",
                    )
                    if not unblocked:
                        logger.error(
                            "next_page_permanently_blocked",
                            target_page=target_page_num,
                        )
                        return False

                    logger.info(
                        "retrying_navigation_after_unblock",
                        target_page=target_page_num,
                    )
                    try:
                        await page.goto(
                            next_url,
                            wait_until="domcontentloaded",
                            timeout=60000,
                        )
                        await asyncio.sleep(ELEMENT_RETRY_WAIT)
                    except Exception as e:
                        logger.warning(
                            "navigation_after_unblock_failed",
                            target_page=target_page_num,
                            error=str(e),
                        )
                        if attempt < MAX_PAGINATION_RETRIES:
                            continue
                        return False

                    still_blocked = (
                        await self._browser_service._check_blocked()
                    )
                    if still_blocked:
                        logger.warning(
                            "blocked_again_after_retry_navigation",
                            target_page=target_page_num,
                        )
                        if attempt < MAX_PAGINATION_RETRIES:
                            continue
                        return False

                container_found = (
                    await self._check_container_after_pagination(
                        page, target_page_num
                    )
                )
                if not container_found:
                    return False

                actual_page_num = self._extract_page_number_from_url(
                    page.url
                )

                if actual_page_num == target_page_num:
                    logger.info(
                        "next_page_loaded",
                        target_page=target_page_num,
                        actual_page=actual_page_num,
                        url=page.url[:200],
                    )
                    return True

                logger.warning(
                    "page_number_mismatch",
                    target_page=target_page_num,
                    actual_page=actual_page_num,
                    attempt=attempt,
                    url=page.url[:200],
                )

                if actual_page_num == 1:
                    logger.warning(
                        "avito_pagination_limit_reached",
                        last_successful_page=current_page_num,
                        attempted_page=target_page_num,
                    )
                    fallback_url = self._build_page_url(
                        current_page_num
                    )
                    if fallback_url:
                        try:
                            await page.goto(
                                fallback_url,
                                wait_until="domcontentloaded",
                                timeout=60000,
                            )
                        except Exception:
                            pass
                    return False

                if attempt < MAX_PAGINATION_RETRIES:
                    logger.info(
                        "retrying_page_navigation",
                        target_page=target_page_num,
                        attempt=attempt,
                    )
                    continue

                return False

            except Exception as e:
                logger.error(
                    "next_page_navigation_failed",
                    target_page=target_page_num,
                    attempt=attempt,
                    error=str(e),
                )

                if attempt < MAX_PAGINATION_RETRIES:
                    await asyncio.sleep(ELEMENT_RETRY_WAIT)
                    continue

                return False

        return False
