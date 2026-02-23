"""Сервис парсинга товаров из каталога Avito.

Извлекает данные товаров из HTML-страниц каталога Avito,
используя CSS-селекторы на основе data-marker атрибутов.
Поддерживает обход пагинации с retry-логикой, обнаружение
циклов и сохранение результатов через репозиторий.
"""

import asyncio
import random
from urllib.parse import parse_qs, urlparse

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
        _visited_urls: Множество посещённых URL для обнаружения циклов.
        _seen_avito_ids: Множество уже встреченных ID товаров.
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
    NEXT_PAGE_BUTTON = "a[data-marker='pagination-button/nextPage']"

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
        self._visited_urls: set[str] = set()
        self._seen_avito_ids: set[str] = set()

    def _normalize_url_for_comparison(self, url: str) -> str:
        """Нормализует URL для сравнения — извлекает параметр p (страница).

        Два URL считаются одинаковыми, если у них совпадают путь
        и параметр p. Остальные параметры (context, f) могут отличаться.

        Args:
            url: Полный URL страницы.

        Returns:
            Нормализованная строка для сравнения (путь + номер страницы).
        """
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        page_num = params.get("p", ["1"])[0]
        return f"{parsed.path}?p={page_num}"

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

    async def scrape_all(self) -> list[RawProduct]:
        """Парсит все страницы категории и сохраняет товары.

        Основной публичный метод. Запускает браузер, переходит на
        первую страницу категории, парсит товары, переходит по
        страницам пагинации до лимита, конца или обнаружения цикла.

        Returns:
            Полный список спарсенных товаров со всех страниц.
        """
        all_products: list[RawProduct] = []
        self._visited_urls.clear()
        self._seen_avito_ids.clear()

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

        # Запоминаем начальный URL
        initial_normalized = self._normalize_url_for_comparison(
            self._settings.category_url
        )
        self._visited_urls.add(initial_normalized)

        await self._browser_service.simulate_human_behavior()

        current_page_num = 1
        max_pages = self._settings.max_pages
        consecutive_empty_pages = 0

        while True:
            logger.info(
                "scraping_page",
                page_number=current_page_num,
                url_page_number=self._extract_page_number_from_url(page.url),
                max_pages=max_pages if max_pages > 0 else "unlimited",
                total_unique_products=len(self._seen_avito_ids),
            )

            products = await self._parse_current_page(page)

            if products:
                # Проверяем, сколько товаров на этой странице — дубликаты
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
                        # Сохраняем оставшиеся новые товары перед выходом
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

            has_next = await self._go_to_next_page(page)
            if not has_next:
                logger.info(
                    "pagination_ended",
                    last_page=current_page_num,
                )
                break

            current_page_num += 1
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
            # Получаем высоту страницы
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

            # Прокручиваем вниз с рандомными шагами
            while current_position < total_height:
                scroll_amount = random.randint(scroll_step_min, scroll_step_max)
                current_position += scroll_amount

                if current_position > total_height:
                    current_position = total_height

                await page.evaluate(
                    f"window.scrollTo(0, {current_position})"
                )

                # Рандомная пауза — имитация чтения
                pause = random.uniform(0.3, 1.2)
                await asyncio.sleep(pause)

                # Иногда небольшая прокрутка назад — как реальный человек
                if random.random() < 0.15:
                    back_scroll = random.randint(50, 150)
                    current_position = max(0, current_position - back_scroll)
                    await page.evaluate(
                        f"window.scrollTo(0, {current_position})"
                    )
                    await asyncio.sleep(random.uniform(0.3, 0.7))

            # Небольшая пауза внизу
            await asyncio.sleep(random.uniform(0.5, 1.5))

            # Возвращаемся наверх плавно
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

        При отсутствии элемента перезагружает страницу и ждёт снова.
        Делает до MAX_ELEMENT_RETRIES попыток с ожиданием
        ELEMENT_RETRY_WAIT секунд между ними.

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

            # Элемент не найден — проверяем, не заблокированы ли мы
            is_blocked = await self._browser_service._check_blocked()
            if is_blocked:
                logger.warning(
                    "element_retry_blocked",
                    element=element_name,
                    attempt=attempt,
                )
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

            # Ждём перед следующей попыткой
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

        # Прокручиваем страницу для подгрузки всех товаров (~10 сек)
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
        # Извлекаем avito_id из атрибута data-item-id
        avito_id = await card.get_attribute("data-item-id")
        if not avito_id:
            logger.debug("card_missing_avito_id")
            return None

        # Название и URL из ссылки заголовка
        title = ""
        url = ""
        title_element = await card.query_selector(self.ITEM_TITLE)
        if title_element:
            title = (await title_element.inner_text()).strip()
            url = await title_element.get_attribute("href") or ""

        if not title:
            logger.debug("card_missing_title", avito_id=avito_id)
            return None

        # Цена из meta-тега
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

        # Описание из meta-тега
        description = ""
        desc_meta = await card.query_selector(self.ITEM_DESCRIPTION)
        if desc_meta:
            description = await desc_meta.get_attribute("content") or ""

        # URL первого изображения
        image_url = ""
        img_element = await card.query_selector(self.ITEM_IMAGE)
        if img_element:
            image_url = await img_element.get_attribute("src") or ""

        # Информация о продавце
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

    async def _find_next_page_url_with_retry(self, page: Page) -> str:
        """Ищет кнопку пагинации с повторными попытками.

        При отсутствии кнопки «Следующая страница» перезагружает
        страницу и ждёт 15 секунд, до 10 попыток. Если кнопка
        найдена — извлекает href и возвращает полный URL.
        Проверяет, не ведёт ли ссылка на уже посещённую страницу.

        Args:
            page: Активная страница Playwright.

        Returns:
            Полный URL следующей страницы или пустая строка,
            если кнопка не найдена после всех попыток или
            обнаружен цикл пагинации.
        """
        for attempt in range(1, MAX_ELEMENT_RETRIES + 1):
            # Пытаемся найти кнопку
            next_button = await page.query_selector(self.NEXT_PAGE_BUTTON)

            if next_button is not None:
                next_href = await next_button.get_attribute("href")
                if next_href:
                    # Формируем полный URL
                    if next_href.startswith("/"):
                        full_url = f"https://www.avito.ru{next_href}"
                    elif next_href.startswith("http"):
                        full_url = next_href
                    else:
                        full_url = f"https://www.avito.ru/{next_href}"

                    # Проверяем, не посещали ли мы эту страницу
                    normalized = self._normalize_url_for_comparison(full_url)
                    if normalized in self._visited_urls:
                        logger.warning(
                            "pagination_cycle_detected_by_url",
                            next_url=full_url[:150],
                            normalized=normalized,
                        )
                        return ""

                    # Запоминаем URL
                    self._visited_urls.add(normalized)

                    logger.info(
                        "next_page_button_found",
                        attempt=attempt,
                        next_url=full_url[:150],
                        page_number=self._extract_page_number_from_url(
                            full_url
                        ),
                    )
                    return full_url

                logger.warning(
                    "next_page_button_no_href",
                    attempt=attempt,
                )

            # Кнопка не найдена — проверяем блокировку
            is_blocked = await self._browser_service._check_blocked()
            if is_blocked:
                logger.warning(
                    "pagination_retry_blocked",
                    attempt=attempt,
                )
                return ""

            if attempt >= MAX_ELEMENT_RETRIES:
                logger.info(
                    "next_page_button_not_found_after_retries",
                    attempts=MAX_ELEMENT_RETRIES,
                    url=page.url,
                )
                return ""

            logger.warning(
                "next_page_button_not_found_retrying",
                attempt=attempt,
                max_attempts=MAX_ELEMENT_RETRIES,
                wait_seconds=ELEMENT_RETRY_WAIT,
                url=page.url,
            )

            # Перезагружаем страницу
            try:
                current_url = page.url
                await page.reload(wait_until="domcontentloaded")
                logger.info(
                    "page_reloaded_for_pagination",
                    attempt=attempt,
                    url=current_url,
                )
            except Exception as e:
                logger.warning(
                    "page_reload_failed_for_pagination",
                    attempt=attempt,
                    error=str(e),
                )

            # Ждём перед следующей попыткой
            await asyncio.sleep(ELEMENT_RETRY_WAIT)

        return ""

    async def _go_to_next_page(self, page: Page) -> bool:
        """Переходит на следующую страницу пагинации.

        Ищет кнопку «Следующая страница» с retry-логикой (до 10 попыток
        с перезагрузкой страницы). Извлекает href и переходит через
        page.goto(). После перехода ждёт 15 секунд и проверяет
        появление каталога с retry. Обнаруживает циклы по URL
        и по дубликатам товаров.

        Args:
            page: Активная страница Playwright.

        Returns:
            True если переход на следующую страницу успешен и каталог найден.
        """
        try:
            # Ищем URL следующей страницы с retry
            next_url = await self._find_next_page_url_with_retry(page)
            if not next_url:
                return False

            # Случайная задержка перед переходом
            delay = random.uniform(2.0, 5.0)
            logger.info(
                "next_page_navigating",
                next_url=next_url[:150],
                delay=round(delay, 1),
            )
            await asyncio.sleep(delay)

            # Переходим на следующую страницу через goto
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

            # Проверяем блокировку
            is_blocked = await self._browser_service._check_blocked()
            if is_blocked:
                logger.warning("next_page_blocked", url=next_url)
                return False

            # Ожидаем появления каталога с retry
            catalog_found = await self._wait_for_element_with_retry(
                page,
                self.CATALOG_CONTAINER,
                "catalog_after_pagination",
            )
            if not catalog_found:
                logger.warning(
                    "next_page_catalog_not_found",
                    url=next_url,
                )
                return False

            logger.info(
                "next_page_loaded",
                url=page.url,
                url_page_number=self._extract_page_number_from_url(page.url),
            )
            return True

        except Exception as e:
            logger.error(
                "next_page_navigation_failed",
                exc_info=True,
                error=str(e),
            )
            return False
