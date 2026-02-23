"""Сервис парсинга товаров из каталога Avito.

Извлекает данные товаров из HTML-страниц каталога Avito,
используя CSS-селекторы на основе data-marker атрибутов.
Поддерживает обход пагинации и сохранение результатов
через репозиторий.
"""

import asyncio
import random

from playwright.async_api import Page

from src.config import ScraperSettings, get_logger
from src.models import RawProduct
from src.repositories.base import BaseProductRepository
from src.services.browser_service import BrowserService

logger = get_logger("scraper_service")


class ScraperService:
    """Сервис для парсинга товаров из каталога Avito.

    Координирует работу BrowserService для навигации и извлекает
    структурированные данные товаров из HTML-разметки страниц.
    Поддерживает пагинацию и батчевое сохранение через репозиторий.

    Attributes:
        _browser_service: Сервис управления браузером.
        _repository: Репозиторий для сохранения товаров.
        _settings: Настройки парсера (URL категории, лимит страниц).
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

    async def scrape_all(self) -> list[RawProduct]:
        """Парсит все страницы категории и сохраняет товары.

        Основной публичный метод. Запускает браузер, переходит на
        первую страницу категории, парсит товары, переходит по
        страницам пагинации до лимита или до конца.

        Returns:
            Полный список спарсенных товаров со всех страниц.
        """
        all_products: list[RawProduct] = []

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

        current_page_num = 1
        max_pages = self._settings.max_pages

        while True:
            logger.info(
                "scraping_page",
                page_number=current_page_num,
                max_pages=max_pages if max_pages > 0 else "unlimited",
            )

            products = await self._parse_current_page(page)

            if products:
                self._repository.save_raw_products(products)
                all_products.extend(products)

                logger.info(
                    "page_scraped",
                    page_number=current_page_num,
                    items_on_page=len(products),
                    total_items=len(all_products),
                )
            else:
                logger.warning(
                    "no_items_on_page",
                    page_number=current_page_num,
                )

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
        )

        return all_products

    async def _parse_current_page(self, page: Page) -> list[RawProduct]:
        """Парсит все карточки товаров на текущей странице.

        Ожидает появления контейнера каталога, затем извлекает данные
        из каждой карточки товара.

        Args:
            page: Активная страница Playwright.

        Returns:
            Список товаров, извлечённых с текущей страницы.
        """
        products: list[RawProduct] = []

        try:
            await page.wait_for_selector(
                self.CATALOG_CONTAINER,
                timeout=15000,
            )
        except Exception:
            logger.warning("catalog_container_not_found")
            return products

        item_cards = await page.query_selector_all(self.ITEM_CARD)

        if not item_cards:
            logger.warning("no_item_cards_found")
            return products

        logger.debug(
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

    async def _go_to_next_page(self, page: Page) -> bool:
        """Переходит на следующую страницу пагинации.

        Ищет кнопку «Следующая страница», кликает по ней и ожидает
        загрузки нового контента. Добавляет случайную задержку
        для имитации человеческого поведения.

        Args:
            page: Активная страница Playwright.

        Returns:
            True если переход на следующую страницу успешен.
        """
        try:
            next_button = await page.query_selector(self.NEXT_PAGE_BUTTON)
            if next_button is None:
                logger.debug("next_page_button_not_found")
                return False

            delay = random.uniform(1.5, 3.5)
            logger.debug(
                "next_page_delay",
                delay=round(delay, 1),
            )
            await asyncio.sleep(delay)

            await next_button.click()

            await page.wait_for_load_state("domcontentloaded")
            await asyncio.sleep(random.uniform(3, 6))

            is_blocked = await self._browser_service._check_blocked()
            if is_blocked:
                logger.warning("next_page_blocked")
                return False

            logger.info("next_page_loaded", url=page.url)
            return True

        except Exception as e:
            logger.error(
                "next_page_navigation_failed",
                exc_info=True,
                error=str(e),
            )
            return False
