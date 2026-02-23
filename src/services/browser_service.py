"""Сервис управления Playwright-браузером.

Инкапсулирует запуск браузера, создание контекста со stealth-настройками,
имитацию человеческого поведения и управление жизненным циклом.
Основан на шаблоне с антидетект-параметрами для обхода защиты Avito.
"""

import asyncio
import random

from playwright.async_api import Browser, BrowserContext, Page, async_playwright

from src.config import BrowserSettings, get_logger

logger = get_logger("browser_service")

# Список User-Agent для ротации при каждом запуске
USER_AGENTS: list[str] = [
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/119.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
]

# JavaScript для сокрытия признаков автоматизации
STEALTH_SCRIPT: str = """
    Object.defineProperty(navigator, 'webdriver', {
        get: () => false,
    });

    window.chrome = {
        runtime: {
            onConnect: null,
            onMessage: null
        }
    };

    Object.defineProperty(navigator, 'plugins', {
        get: () => [1, 2, 3, 4, 5]
    });

    Object.defineProperty(navigator, 'languages', {
        get: () => ['ru-RU', 'ru', 'en-US', 'en']
    });

    Object.defineProperty(navigator, 'hardwareConcurrency', {
        get: () => 4
    });

    Object.defineProperty(navigator, 'deviceMemory', {
        get: () => 8
    });
"""

# Аргументы запуска Chromium для антидетекта
BROWSER_ARGS: list[str] = [
    "--disable-blink-features=AutomationControlled",
    "--exclude-switches=enable-automation",
    "--disable-extensions",
    "--disable-dev-shm-usage",
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-web-security",
    "--disable-features=TranslateUI,VizDisplayCompositor",
    "--disable-background-timer-throttling",
    "--disable-backgrounding-occluded-windows",
    "--disable-renderer-backgrounding",
    "--disable-field-trial-config",
    "--disable-back-forward-cache",
    "--disable-ipc-flooding-protection",
    "--disable-default-apps",
    "--mute-audio",
    "--no-default-browser-check",
    "--no-first-run",
    "--no-pings",
    "--password-store=basic",
    "--use-mock-keychain",
]


class BrowserService:
    """Сервис для управления Playwright-браузером со stealth-режимом.

    Инкапсулирует всю логику работы с браузером: запуск, настройку
    контекста, stealth-инъекции и имитацию поведения пользователя.

    Attributes:
        _settings: Настройки браузера из конфигурации.
        _playwright: Экземпляр Playwright (async context manager).
        _browser: Экземпляр запущенного браузера.
        _context: Контекст браузера с настройками.
        _page: Активная страница.
    """

    def __init__(self, settings: BrowserSettings) -> None:
        """Инициализирует сервис.

        Args:
            settings: Настройки браузера (headless, таймауты).
        """
        self._settings = settings
        self._playwright: object | None = None
        self._browser: Browser | None = None
        self._context: BrowserContext | None = None
        self._page: Page | None = None

    async def launch(self) -> Page:
        """Запускает браузер, создаёт контекст и страницу.

        Полный цикл инициализации: запуск Chromium с антидетект-аргументами,
        создание контекста с рандомизированным User-Agent и viewport,
        инъекция stealth-скриптов.

        Returns:
            Готовая к использованию страница Playwright.

        Raises:
            RuntimeError: Если не удалось запустить браузер.
        """
        try:
            pw = await async_playwright().start()
            self._playwright = pw

            self._browser = await pw.chromium.launch(
                headless=self._settings.headless,
                args=BROWSER_ARGS,
            )

            selected_ua = random.choice(USER_AGENTS)
            logger.info(
                "browser_user_agent_selected",
                user_agent=selected_ua[:60],
            )

            self._context = await self._browser.new_context(
                user_agent=selected_ua,
                viewport={
                    "width": random.randint(1024, 1920),
                    "height": random.randint(768, 1080),
                },
                locale="ru-RU",
                timezone_id="Europe/Moscow",
                geolocation={"longitude": 37.6175, "latitude": 55.7558},
                permissions=["geolocation"],
                color_scheme="light",
                device_scale_factor=1,
                is_mobile=False,
                has_touch=False,
                extra_http_headers={
                    "Accept": (
                        "text/html,application/xhtml+xml,"
                        "application/xml;q=0.9,image/webp,*/*;q=0.8"
                    ),
                    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120"',
                    "Sec-Ch-Ua-Mobile": "?0",
                    "Sec-Ch-Ua-Platform": '"Windows"',
                    "Sec-Fetch-Dest": "document",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Site": "none",
                    "Sec-Fetch-User": "?1",
                    "Upgrade-Insecure-Requests": "1",
                },
            )

            self._context.set_default_timeout(
                self._settings.navigation_timeout
            )
            self._context.set_default_navigation_timeout(
                self._settings.navigation_timeout
            )

            self._page = await self._context.new_page()
            await self._page.add_init_script(STEALTH_SCRIPT)

            logger.info(
                "browser_launched",
                headless=self._settings.headless,
                timeout=self._settings.navigation_timeout,
            )

            return self._page

        except Exception as e:
            logger.error(
                "browser_launch_failed",
                exc_info=True,
                error=str(e),
            )
            await self.close()
            raise RuntimeError(
                f"Не удалось запустить браузер: {e}"
            ) from e

    async def navigate(self, url: str) -> bool:
        """Переходит по URL с предварительной задержкой и проверкой блокировки.

        Имитирует поведение реального пользователя: случайная задержка
        перед переходом, ожидание CloudFlare challenge, проверка
        блокировки по заголовку и содержимому страницы.

        Args:
            url: URL для перехода.

        Returns:
            True если страница загружена успешно и без блокировки.
        """
        if self._page is None:
            logger.error("navigate_no_page", url=url)
            return False

        try:
            delay = random.uniform(2, 4)
            logger.info(
                "navigation_started",
                url=url,
                delay=round(delay, 1),
            )
            await asyncio.sleep(delay)

            await self._page.goto(
                url,
                wait_until="domcontentloaded",
                timeout=self._settings.navigation_timeout,
            )

            # Ожидание для CloudFlare
            await asyncio.sleep(5)

            is_blocked = await self._check_blocked()
            if is_blocked:
                logger.warning("page_blocked", url=url)
                return False

            logger.info(
                "navigation_success",
                url=url,
                current_url=self._page.url,
            )
            return True

        except Exception as e:
            logger.error(
                "navigation_failed",
                exc_info=True,
                url=url,
                error=str(e),
            )
            return False

    async def _check_blocked(self) -> bool:
        """Проверяет, заблокирован ли доступ к странице.

        Анализирует заголовок страницы и содержимое body на наличие
        признаков блокировки или CloudFlare challenge.

        Returns:
            True если обнаружена блокировка.
        """
        if self._page is None:
            return True

        try:
            title = await self._page.title()

            if "Just a moment" in title:
                logger.info("cloudflare_challenge_detected")
                await asyncio.sleep(10)
                title = await self._page.title()

            blocked_titles = ("Доступ ограничен", "Access denied")
            if any(phrase in title for phrase in blocked_titles):
                return True

            body_text = await self._page.text_content("body")
            if body_text is None:
                return False

            blocked_keywords = (
                "blocked",
                "captcha",
                "access denied",
                "forbidden",
            )
            return any(
                keyword in body_text.lower() for keyword in blocked_keywords
            )

        except Exception as e:
            logger.warning(
                "block_check_failed",
                error=str(e),
            )
            return False

    async def simulate_human_behavior(self) -> None:
        """Имитирует поведение реального пользователя на странице.

        Выполняет случайные движения мыши и прокрутку страницы,
        чтобы снизить вероятность обнаружения автоматизации.
        """
        if self._page is None:
            return

        try:
            for _ in range(3):
                x = random.randint(100, 800)
                y = random.randint(100, 600)
                await self._page.mouse.move(x, y)
                await asyncio.sleep(random.uniform(0.5, 1.5))

            await self._page.evaluate(
                "window.scrollTo(0, document.body.scrollHeight / 4)"
            )
            await asyncio.sleep(random.uniform(1, 2))

            await self._page.evaluate(
                "window.scrollTo(0, document.body.scrollHeight / 2)"
            )
            await asyncio.sleep(random.uniform(1, 2))

            await self._page.evaluate("window.scrollTo(0, 0)")
            await asyncio.sleep(1)

            logger.debug("human_behavior_simulated")

        except Exception as e:
            logger.warning(
                "human_behavior_simulation_failed",
                error=str(e),
            )

    async def wait(self, milliseconds: int | None = None) -> None:
        """Ожидает указанное время на текущей странице.

        Args:
            milliseconds: Время ожидания в миллисекундах.
                Если None — используется значение из настроек.
        """
        if self._page is None:
            return

        wait_ms = milliseconds or self._settings.page_wait_time
        logger.debug("page_wait_started", wait_ms=wait_ms)
        await self._page.wait_for_timeout(wait_ms)

    @property
    def page(self) -> Page | None:
        """Возвращает текущую активную страницу.

        Returns:
            Экземпляр Page или None если браузер не запущен.
        """
        return self._page

    async def close(self) -> None:
        """Закрывает браузер и освобождает все ресурсы.

        Безопасно закрывает контекст, браузер и Playwright
        в правильном порядке, подавляя ошибки при закрытии.
        """
        try:
            if self._context is not None:
                await self._context.close()
                self._context = None

            if self._browser is not None:
                await self._browser.close()
                self._browser = None

            if self._playwright is not None:
                await self._playwright.stop()  # type: ignore[union-attr]
                self._playwright = None

            self._page = None
            logger.info("browser_closed")

        except Exception as e:
            logger.warning(
                "browser_close_error",
                error=str(e),
            )
