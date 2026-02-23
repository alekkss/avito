"""Сервис парсинга товаров из каталога Avito.

Извлекает данные товаров из HTML-страниц каталога Avito,
используя CSS-селекторы на основе data-marker атрибутов.
Поддерживает обход пагинации с retry-логикой, обнаружение
циклов и сохранение результатов через репозиторий.
"""

import asyncio
import random
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from playwright.async_api import Page

from src.config import ScraperSettings, get_logger
from src.models import RawProduct
from src.repositories.base import BaseProductRepository
from src.services.browser_service import BrowserService

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


class ScraperService:
    """Сервис для парсинга товаров из каталога Avito.

    Координирует работу BrowserService для навигации и извлекает
    структурированные данные товаров из HTML-разметки страниц.
    Поддерживает пагинацию, обнаружение циклов и батчевое
    сохранение через репозиторий.

    Attributes:
        _browser_service: Сервис управления браузером.
        _repository: Репозиторий для сохранения товаров.
        _settings: Настройки парсера (URL категории, лимит страниц).
        _seen_avito_ids: Множество уже встреченных ID товаров.
        _total_pages: Общее количество страниц (определяется из пагинации).
        _base_url: Базовый URL категории (без параметра p), используется
            для конструирования URL страниц пагинации.
    """

    # CSS-селекторы для элементов каталога Avito
    CATALOG_CONTAINER = "div[data-marker='catalog-serp']"
    ITEM_CARD = "div[data-marker='item']"
    ITEM_TITLE = "a[data-marker='item-title']"
    ITEM_PRICE_META = "meta[itemprop='price']"
    ITEM_DESCRIPTION = "meta[itemprop='description']"
    ITEM_IMAGE = "img[itemprop='image']"
    SELLER_RATING = "[data-marker='seller-rating/score']"
    SELLER_REVIEWS = "[data-marker='seller-info/summary']"

    def __init__(
        self,
        browser_service: BrowserService,
        repository: BaseProductRepository,
        settings: ScraperSettings,
    ) -> None:
        """Инициализирует сервис парсинга.

        Args:
            browser_service: Сервис для управления браузером.
            repository: Репозиторий для сохранения спарсенных товаров.
            settings: Настройки парсера (URL, лимит страниц).
        """
        self._browser_service = browser_service
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

        Avito может добавить дополнительные параметры (context, f)
        при первой загрузке страницы. Мы захватываем реальный URL
        из браузера (уже с этими параметрами) и используем его
        как базу для конструирования URL следующих страниц.
        Параметр p удаляется — он будет подставляться отдельно.

        Args:
            url: Реальный URL из адресной строки браузера.
        """
        parsed = urlparse(url)
        params = parse_qs(parsed.query, keep_blank_values=True)

        # Удаляем параметр p — будем подставлять его сами
        params.pop("p", None)

        # Собираем обратно без p
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

        Берёт базовый URL (захваченный из браузера после первой
        загрузки) и добавляет параметр p=N. Для страницы 1
        параметр p не добавляется (Avito так работает по умолчанию).

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

        Ищет все номерные кнопки пагинации и находит максимальный
        номер среди них. Это последняя доступная страница.

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

        При обнаружении блокировки ждёт UNBLOCK_WAIT секунд,
        перезагружает страницу и проверяет title снова.
        Повторяет до MAX_UNBLOCK_RETRIES раз.

        Args:
            page: Активная страница Playwright.
            context: Контекст вызова для логирования (например,
                'pagination', 'element_wait').

        Returns:
            True если блокировка снята и страница загружена нормально.
            False если после всех попыток страница всё ещё заблокирована.
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

            # Перезагружаем страницу
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

            # Ждём после перезагрузки, чтобы страница успела отрендериться
            await asyncio.sleep(5)

            # Проверяем, снята ли блокировка
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

    async def scrape_all(self) -> list[RawProduct]:
        """Парсит все страницы категории и сохраняет товары.

        Основной публичный метод. Запускает браузер, переходит на
        первую страницу категории, парсит товары, переходит по
        страницам пагинации до лимита, конца или обнаружения цикла.

        Returns:
            Полный список спарсенных товаров со всех страниц.
        """
        all_products: list[RawProduct] = []
        self._seen_avito_ids.clear()
        self._total_pages = 0
        self._base_url = ""

        page = await self._browser_service.launch()

        success = await self._browser_service.navigate(
            self._settings.category_url
        )
        if not success:
            logger.error(
                "initial_navigation_failed",
                url=self._settings.category_url,
            )
            return all_products

        await self._browser_service.simulate_human_behavior()

        # Захватываем базовый URL после загрузки первой страницы
        self._capture_base_url(page.url)

        # Определяем общее количество страниц из пагинации
        self._total_pages = await self._detect_total_pages(page)

        current_page_num = 1
        max_pages = self._settings.max_pages
        consecutive_empty_pages = 0

        while True:
            logger.info(
                "scraping_page",
                page_number=current_page_num,
                url_page_number=self._extract_page_number_from_url(page.url),
                max_pages=max_pages if max_pages > 0 else "unlimited",
                total_pages=self._total_pages if self._total_pages > 0 else "unknown",
                total_unique_products=len(self._seen_avito_ids),
            )

            products = await self._parse_current_page(page)

            if products:
                new_products: list[RawProduct] = []
                duplicate_count = 0

                for product in products:
                    if product.avito_id in self._seen_avito_ids:
                        duplicate_count += 1
                    else:
                        self._seen_avito_ids.add(product.avito_id)
                        new_products.append(product)

                # Обнаружение цикла: если большинство товаров — дубликаты
                if len(products) > 0:
                    duplicate_ratio = duplicate_count / len(products)
                    if duplicate_ratio >= DUPLICATE_THRESHOLD:
                        logger.warning(
                            "pagination_cycle_detected",
                            page_number=current_page_num,
                            total_items=len(products),
                            duplicates=duplicate_count,
                            duplicate_ratio=f"{duplicate_ratio:.0%}",
                        )
                        if new_products:
                            self._repository.save_raw_products(new_products)
                            all_products.extend(new_products)
                        break

                if new_products:
                    self._repository.save_raw_products(new_products)
                    all_products.extend(new_products)

                consecutive_empty_pages = 0

                logger.info(
                    "page_scraped",
                    page_number=current_page_num,
                    items_on_page=len(products),
                    new_items=len(new_products),
                    duplicates=duplicate_count,
                    total_items=len(all_products),
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

            # Проверяем, не достигли ли последней страницы
            if self._total_pages > 0 and current_page_num >= self._total_pages:
                logger.info(
                    "last_page_reached",
                    current_page=current_page_num,
                    total_pages=self._total_pages,
                )
                break

            has_next = await self._go_to_next_page(page, current_page_num)
            if not has_next:
                logger.info(
                    "pagination_ended",
                    last_page=current_page_num,
                )
                break

            current_page_num += 1

            # Обновляем общее количество страниц (может измениться)
            updated_total = await self._detect_total_pages(page)
            if updated_total > 0:
                self._total_pages = updated_total

            await self._browser_service.simulate_human_behavior()

        logger.info(
            "scraping_completed",
            total_pages=current_page_num,
            total_items=len(all_products),
            unique_avito_ids=len(self._seen_avito_ids),
        )

        return all_products

    async def _scroll_page_naturally(self, page: Page) -> None:
        """Прокручивает страницу как реальный пользователь (~10 секунд).

        Последовательно прокручивает страницу вниз небольшими шагами
        с рандомными паузами, имитируя чтение контента. Затем
        возвращается наверх. Это помогает подгрузить lazy-loaded
        контент и снижает риск обнаружения автоматизации.

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

            logger.debug(
                "scroll_started",
                total_height=total_height,
                viewport_height=viewport_height,
            )

            while current_position < total_height:
                scroll_amount = random.randint(scroll_step_min, scroll_step_max)
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
                    current_position = max(0, current_position - back_scroll)
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

        При отсутствии элемента проверяет блокировку. Если страница
        заблокирована — запускает цикл ожидания разблокировки
        (до MAX_UNBLOCK_RETRIES попыток с перезагрузкой).
        Если не заблокирована — перезагружает страницу и ждёт снова.
        Делает до MAX_ELEMENT_RETRIES общих попыток.

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

            # Элемент не найден — проверяем блокировку
            is_blocked = await self._browser_service._check_blocked()
            if is_blocked:
                # Запускаем цикл ожидания разблокировки
                unblocked = await self._wait_for_unblock(
                    page,
                    context=f"element_wait:{element_name}",
                )
                if unblocked:
                    # Блокировка снята — пробуем найти элемент снова
                    # (не тратим попытку, просто продолжаем цикл)
                    continue
                # Блокировка не снята после всех попыток
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

            # Перезагружаем текущую страницу
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

    async def _parse_current_page(self, page: Page) -> list[RawProduct]:
        """Парсит все карточки товаров на текущей странице.

        Ожидает появления контейнера каталога с повторными попытками,
        прокручивает страницу для подгрузки lazy-контента, затем
        извлекает данные из каждой карточки.

        Args:
            page: Активная страница Playwright.

        Returns:
            Список товаров, извлечённых с текущей страницы.
        """
        products: list[RawProduct] = []

        catalog_found = await self._wait_for_element_with_retry(
            page,
            self.CATALOG_CONTAINER,
            "catalog_container",
        )
        if not catalog_found:
            logger.warning("catalog_container_not_found_after_retries")
            return products

        await self._scroll_page_naturally(page)

        item_cards = await page.query_selector_all(self.ITEM_CARD)

        if not item_cards:
            logger.warning("no_item_cards_found")
            return products

        logger.info(
            "item_cards_found",
            count=len(item_cards),
        )

        for card in item_cards:
            try:
                product = await self._parse_single_card(card)
                if product is not None:
                    products.append(product)
            except Exception as e:
                logger.warning(
                    "card_parse_error",
                    error=str(e),
                    error_type=type(e).__name__,
                )

        return products

    async def _parse_single_card(
        self, card: object
    ) -> RawProduct | None:
        """Извлекает данные одного товара из карточки.

        Парсит HTML-элемент карточки товара, извлекая название, цену,
        ссылку, описание, фото и информацию о продавце.

        Args:
            card: ElementHandle карточки товара.

        Returns:
            RawProduct с данными товара или None если не удалось распарсить.
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

        description = ""
        desc_meta = await card.query_selector(self.ITEM_DESCRIPTION)
        if desc_meta:
            description = await desc_meta.get_attribute("content") or ""

        image_url = ""
        img_element = await card.query_selector(self.ITEM_IMAGE)
        if img_element:
            image_url = await img_element.get_attribute("src") or ""

        seller_name = await self._extract_seller_name(card)
        seller_rating = await self._extract_text_by_selector(
            card, self.SELLER_RATING
        )
        seller_reviews = await self._extract_text_by_selector(
            card, self.SELLER_REVIEWS
        )

        product = RawProduct(
            avito_id=avito_id,
            title=title,
            price=price,
            url=url,
            description=description,
            image_url=image_url,
            seller_name=seller_name,
            seller_rating=seller_rating,
            seller_reviews=seller_reviews,
        )

        logger.debug(
            "product_parsed",
            avito_id=avito_id,
            title=title[:50],
            price=price,
        )

        return product

    async def _extract_seller_name(self, card: object) -> str:
        """Извлекает имя продавца из карточки товара.

        Продавец находится внутри блока iva-item-sellerInfo
        в первом теге <p> внутри ссылки <a>.

        Args:
            card: ElementHandle карточки товара.

        Returns:
            Имя продавца или пустая строка.
        """
        try:
            seller_block = await card.query_selector(
                "div[class*='iva-item-sellerInfo'] a p"
            )
            if seller_block:
                text = (await seller_block.inner_text()).strip()
                return text
        except Exception:
            pass
        return ""

    async def _extract_text_by_selector(
        self, card: object, selector: str
    ) -> str:
        """Извлекает текстовое содержимое элемента по CSS-селектору.

        Args:
            card: ElementHandle карточки товара.
            selector: CSS-селектор элемента.

        Returns:
            Текст элемента или пустая строка если не найден.
        """
        try:
            element = await card.query_selector(selector)
            if element:
                text = (await element.inner_text()).strip()
                return text
        except Exception:
            pass
        return ""

    async def _go_to_next_page(
        self, page: Page, current_page_num: int
    ) -> bool:
        """Переходит на следующую страницу пагинации.

        Конструирует URL следующей страницы самостоятельно, подставляя
        параметр p=N в базовый URL. После перехода валидирует, что
        загруженная страница соответствует ожидаемому номеру.

        При обнаружении блокировки запускает цикл ожидания
        разблокировки (до MAX_UNBLOCK_RETRIES попыток с перезагрузкой)
        вместо немедленного завершения.

        Args:
            page: Активная страница Playwright.
            current_page_num: Номер текущей страницы.

        Returns:
            True если переход на следующую страницу успешен и валиден.
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
                # Случайная задержка перед переходом
                delay = random.uniform(2.0, 5.0)
                logger.info(
                    "next_page_navigating",
                    target_page=target_page_num,
                    attempt=attempt,
                    delay=round(delay, 1),
                )
                await asyncio.sleep(delay)

                # Переходим на сконструированный URL
                await page.goto(
                    next_url,
                    wait_until="domcontentloaded",
                    timeout=60000,
                )

                # Ожидание после загрузки DOM
                logger.info(
                    "next_page_waiting",
                    wait_seconds=ELEMENT_RETRY_WAIT,
                )
                await asyncio.sleep(ELEMENT_RETRY_WAIT)

                # Проверяем блокировку — с ожиданием разблокировки
                is_blocked = await self._browser_service._check_blocked()
                if is_blocked:
                    unblocked = await self._wait_for_unblock(
                        page,
                        context=f"pagination:page_{target_page_num}",
                    )
                    if not unblocked:
                        # После всех попыток всё ещё заблокированы
                        logger.error(
                            "next_page_permanently_blocked",
                            target_page=target_page_num,
                        )
                        return False

                    # Блокировка снята — нужно снова перейти на целевую страницу,
                    # потому что reload мог загрузить страницу блокировки
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

                    # Проверяем блокировку ещё раз после повторного перехода
                    still_blocked = await self._browser_service._check_blocked()
                    if still_blocked:
                        logger.warning(
                            "blocked_again_after_retry_navigation",
                            target_page=target_page_num,
                        )
                        if attempt < MAX_PAGINATION_RETRIES:
                            continue
                        return False

                # Ожидаем появления каталога
                catalog_found = await self._wait_for_element_with_retry(
                    page,
                    self.CATALOG_CONTAINER,
                    "catalog_after_pagination",
                )
                if not catalog_found:
                    logger.warning(
                        "next_page_catalog_not_found",
                        target_page=target_page_num,
                        attempt=attempt,
                    )
                    if attempt < MAX_PAGINATION_RETRIES:
                        continue
                    return False

                # ВАЛИДАЦИЯ: проверяем, что реальный URL соответствует
                # ожидаемому номеру страницы
                actual_page_num = self._extract_page_number_from_url(page.url)

                if actual_page_num == target_page_num:
                    logger.info(
                        "next_page_loaded",
                        target_page=target_page_num,
                        actual_page=actual_page_num,
                        url=page.url[:200],
                    )
                    return True

                # Avito сбросил на другую страницу
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
                    fallback_url = self._build_page_url(current_page_num)
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
