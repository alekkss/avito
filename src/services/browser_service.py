"""Сервис управления Playwright-браузером.

Инкапсулирует запуск браузера, создание контекста со stealth-настройками,
имитацию человеческого поведения, ротацию прокси и управление жизненным
циклом. Основан на шаблоне с антидетект-параметрами для обхода защиты Avito.

Поддерживает два режима работы:
- Автономный (launch): загружает прокси из файла, запускает свой Chromium.
- Воркер (launch_for_worker): использует переданный Browser-инстанс
  и назначенные прокси, не управляет жизненным циклом Playwright/Browser.
"""

import asyncio
import random
from dataclasses import dataclass
from pathlib import Path

from playwright.async_api import (
    Browser,
    BrowserContext,
    Page,
    Playwright,
    async_playwright,
)

from src.config import BrowserSettings, ProxySettings, get_logger

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
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (X11; Linux x86_64) "
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

# Максимальное количество попыток пройти CloudFlare challenge
MAX_CLOUDFLARE_RETRIES: int = 3
CLOUDFLARE_WAIT_SECONDS: int = 15

# Ключевые фразы сетевых ошибок прокси — при их обнаружении
# в тексте исключения навигации прокси считается нерабочим
PROXY_ERROR_MARKERS: tuple[str, ...] = (
    "ERR_TUNNEL_CONNECTION_FAILED",
    "ERR_PROXY_CONNECTION_FAILED",
    "ERR_PROXY_AUTH_FAILED",
    "ERR_SOCKS_CONNECTION_FAILED",
    "ERR_CONNECTION_RESET",
    "ERR_CONNECTION_REFUSED",
    "ERR_CONNECTION_TIMED_OUT",
    "ERR_CONNECTION_CLOSED",
    "PROXY_CONNECTION_FAILED",
)

# Максимальное количество смен прокси при навигации на каталог
MAX_PROXY_RETRIES_ON_NAVIGATE: int = 10


@dataclass
class ProxyInfo:
    """Данные одного прокси-сервера.

    Attributes:
        server: URL прокси в формате http://host:port.
        username: Имя пользователя для авторизации.
        password: Пароль для авторизации.
    """

    server: str
    username: str
    password: str


def load_proxies_from_file(file_path: str) -> list[ProxyInfo]:
    """Загружает список прокси из текстового файла.

    Формат каждой строки: host:port:username:password.
    Пустые строки и строки, начинающиеся с #, пропускаются.

    Функция вынесена на уровень модуля, чтобы ParallelListingService
    мог загрузить прокси один раз и распределить между воркерами.

    Args:
        file_path: Путь к файлу с прокси.

    Returns:
        Список объектов ProxyInfo.

    Raises:
        RuntimeError: Если файл не содержит валидных прокси.
    """
    proxies: list[ProxyInfo] = []
    path = Path(file_path)

    if not path.exists():
        raise RuntimeError(
            f"Файл прокси не найден: {file_path}"
        )

    with open(path, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, start=1):
            line = line.strip()

            # Пропускаем пустые строки и комментарии
            if not line or line.startswith("#"):
                continue

            parts = line.split(":")
            if len(parts) != 4:
                logger.warning(
                    "proxy_line_invalid_format",
                    line_num=line_num,
                    expected="host:port:user:pass",
                )
                continue

            host, port, username, password = parts

            try:
                int(port)
            except ValueError:
                logger.warning(
                    "proxy_line_invalid_port",
                    line_num=line_num,
                    port=port,
                )
                continue

            proxies.append(ProxyInfo(
                server=f"http://{host}:{port}",
                username=username,
                password=password,
            ))

    if not proxies:
        raise RuntimeError(
            f"Файл прокси '{file_path}' не содержит валидных записей. "
            f"Формат строки: host:port:user:pass"
        )

    logger.info(
        "proxies_loaded",
        file_path=file_path,
        total=len(proxies),
    )

    return proxies


def _is_proxy_error(error_text: str) -> bool:
    """Проверяет, является ли ошибка навигации проблемой прокси.

    Анализирует текст исключения на наличие характерных
    маркеров сетевых ошибок прокси-соединения.

    Args:
        error_text: Текст ошибки (str(exception)).

    Returns:
        True если ошибка вызвана нерабочим прокси.
    """
    error_upper = error_text.upper()
    return any(marker in error_upper for marker in PROXY_ERROR_MARKERS)


class BrowserService:
    """Сервис для управления Playwright-браузером со stealth-режимом.

    Инкапсулирует всю логику работы с браузером: запуск, настройку
    контекста, stealth-инъекции, ротацию прокси и имитацию поведения
    пользователя.

    Поддерживает два режима:
    - Автономный: вызов launch() создаёт свой Playwright + Browser.
    - Воркер: вызов launch_for_worker() использует переданный Browser,
      создаёт только свой BrowserContext. Жизненным циклом Playwright
      и Browser управляет вызывающий код (ParallelListingService).

    Attributes:
        _settings: Настройки браузера из конфигурации.
        _proxy_settings: Настройки ротации прокси.
        _playwright: Экземпляр Playwright (None в режиме воркера).
        _browser: Экземпляр запущенного браузера.
        _context: Контекст браузера с настройками.
        _page: Активная страница.
        _proxies: Список загруженных/назначенных прокси.
        _current_proxy_index: Индекс текущего прокси в списке.
        _listings_since_rotation: Счётчик карточек с последней ротации.
        _owns_browser: True если этот экземпляр управляет жизненным
            циклом Playwright и Browser (автономный режим).
        _worker_id: Идентификатор воркера для логирования (None если
            автономный режим).
    """

    def __init__(
        self,
        settings: BrowserSettings,
        proxy_settings: ProxySettings,
        assigned_proxies: list[ProxyInfo] | None = None,
        worker_id: int | None = None,
    ) -> None:
        """Инициализирует сервис.

        Args:
            settings: Настройки браузера (headless, таймауты).
            proxy_settings: Настройки прокси (путь к файлу, порог ротации).
            assigned_proxies: Заранее назначенные прокси для воркера.
                Если передан — загрузка из файла пропускается.
                Если None — прокси загружаются из файла при launch().
            worker_id: Идентификатор воркера для логирования.
                None — автономный режим (каталог).
        """
        self._settings = settings
        self._proxy_settings = proxy_settings
        self._playwright: Playwright | None = None
        self._browser: Browser | None = None
        self._context: BrowserContext | None = None
        self._page: Page | None = None
        self._assigned_proxies = assigned_proxies
        self._proxies: list[ProxyInfo] = []
        self._current_proxy_index: int = -1
        self._listings_since_rotation: int = 0
        self._owns_browser: bool = True
        self._worker_id = worker_id

    def _log_prefix(self) -> str:
        """Возвращает префикс для логов с идентификатором воркера.

        Returns:
            Строка вида "worker_3" или "main" для автономного режима.
        """
        if self._worker_id is not None:
            return f"worker_{self._worker_id}"
        return "main"

    def _load_proxies(self) -> None:
        """Загружает прокси из файла или использует назначенные.

        Если в конструктор переданы assigned_proxies — использует их.
        Иначе загружает из файла по пути из настроек.
        При пустом пути — парсер работает без прокси.
        """
        # Если прокси назначены извне (режим воркера) — используем их
        if self._assigned_proxies is not None:
            self._proxies = list(self._assigned_proxies)
            logger.info(
                "proxies_assigned",
                source=self._log_prefix(),
                total=len(self._proxies),
                rotate_every=self._proxy_settings.rotate_every_n,
            )
            return

        # Автономный режим — загружаем из файла
        if not self._proxy_settings.proxy_file_path:
            logger.info(
                "proxy_disabled_no_file",
                source=self._log_prefix(),
            )
            return

        self._proxies = load_proxies_from_file(
            self._proxy_settings.proxy_file_path
        )

        # Перемешиваем для равномерного распределения нагрузки
        random.shuffle(self._proxies)

        logger.info(
            "proxies_ready",
            source=self._log_prefix(),
            total=len(self._proxies),
            rotate_every=self._proxy_settings.rotate_every_n,
        )

    def _get_next_proxy(self) -> ProxyInfo | None:
        """Возвращает следующий прокси из пула.

        Циклически перебирает список прокси. Если прокси
        закончились — начинает сначала.

        Returns:
            Следующий ProxyInfo или None, если прокси не загружены.
        """
        if not self._proxies:
            return None

        self._current_proxy_index += 1

        # Циклический перебор
        if self._current_proxy_index >= len(self._proxies):
            self._current_proxy_index = 0
            logger.info(
                "proxy_pool_recycled",
                source=self._log_prefix(),
                total=len(self._proxies),
            )

        proxy = self._proxies[self._current_proxy_index]

        logger.info(
            "proxy_selected",
            source=self._log_prefix(),
            index=self._current_proxy_index,
            total=len(self._proxies),
            server=proxy.server,
        )

        return proxy

    async def _create_context(
        self, proxy: ProxyInfo | None = None
    ) -> BrowserContext:
        """Создаёт новый контекст браузера со stealth-настройками.

        При каждом создании контекста рандомизируются User-Agent,
        viewport и другие параметры для уникального fingerprint.

        Args:
            proxy: Прокси для контекста (None — без прокси).

        Returns:
            Новый BrowserContext.

        Raises:
            RuntimeError: Если браузер не запущен.
        """
        if self._browser is None:
            raise RuntimeError(
                "Браузер не запущен. Вызовите launch() перед "
                "созданием контекста."
            )

        selected_ua = random.choice(USER_AGENTS)
        viewport_width = random.randint(1024, 1920)
        viewport_height = random.randint(768, 1080)

        logger.info(
            "context_creating",
            source=self._log_prefix(),
            user_agent=selected_ua[:60],
            viewport=f"{viewport_width}x{viewport_height}",
            proxy_server=proxy.server if proxy else "без прокси",
        )

        # Параметры контекста
        context_kwargs: dict = {
            "user_agent": selected_ua,
            "viewport": {
                "width": viewport_width,
                "height": viewport_height,
            },
            "locale": "ru-RU",
            "timezone_id": "Europe/Moscow",
            "geolocation": {
                "longitude": 37.6175,
                "latitude": 55.7558,
            },
            "permissions": ["geolocation"],
            "color_scheme": "light",
            "device_scale_factor": 1,
            "is_mobile": False,
            "has_touch": False,
            "extra_http_headers": {
                "Accept": (
                    "text/html,application/xhtml+xml,"
                    "application/xml;q=0.9,image/webp,*/*;q=0.8"
                ),
                "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
                "Accept-Encoding": "gzip, deflate, br",
                "Sec-Ch-Ua": (
                    '"Not_A Brand";v="8", "Chromium";v="120"'
                ),
                "Sec-Ch-Ua-Mobile": "?0",
                "Sec-Ch-Ua-Platform": '"Windows"',
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1",
                "Upgrade-Insecure-Requests": "1",
            },
        }

        # Добавляем прокси, если указан
        if proxy is not None:
            context_kwargs["proxy"] = {
                "server": proxy.server,
                "username": proxy.username,
                "password": proxy.password,
            }

        context = await self._browser.new_context(**context_kwargs)

        context.set_default_timeout(
            self._settings.navigation_timeout
        )
        context.set_default_navigation_timeout(
            self._settings.navigation_timeout
        )

        return context

    async def launch(self) -> Page:
        """Запускает браузер, создаёт контекст и страницу.

        Автономный режим: загрузка прокси из файла,
        запуск Chromium с антидетект-аргументами, создание контекста
        с первым прокси (или без прокси), инъекция stealth-скриптов.

        Returns:
            Готовая к использованию страница Playwright.

        Raises:
            RuntimeError: Если не удалось запустить браузер.
        """
        try:
            # Загружаем прокси из файла
            self._load_proxies()

            pw = await async_playwright().start()
            self._playwright = pw

            self._browser = await pw.chromium.launch(
                headless=self._settings.headless,
                args=BROWSER_ARGS,
            )

            self._owns_browser = True

            # Берём первый прокси (или None, если прокси отключены)
            first_proxy = self._get_next_proxy()

            self._context = await self._create_context(
                proxy=first_proxy,
            )

            self._page = await self._context.new_page()
            await self._page.add_init_script(STEALTH_SCRIPT)

            # Сбрасываем счётчик карточек
            self._listings_since_rotation = 0

            logger.info(
                "browser_launched",
                source=self._log_prefix(),
                headless=self._settings.headless,
                timeout=self._settings.navigation_timeout,
                proxy_enabled=len(self._proxies) > 0,
                proxy_server=(
                    first_proxy.server if first_proxy else "нет"
                ),
            )

            return self._page

        except Exception as e:
            logger.error(
                "browser_launch_failed",
                source=self._log_prefix(),
                exc_info=True,
                error=str(e),
            )
            await self.close()
            raise RuntimeError(
                f"Не удалось запустить браузер: {e}"
            ) from e

    async def launch_for_worker(self, browser: Browser) -> Page:
        """Создаёт контекст и страницу на переданном Browser-инстансе.

        Режим воркера: не запускает свой Playwright/Browser, а использует
        переданный. Жизненным циклом Browser управляет вызывающий код
        (ParallelListingService). При close() закрывает только свой
        контекст, не трогая Browser.

        Прокси берутся из assigned_proxies, переданных в конструктор.

        Args:
            browser: Уже запущенный Playwright Browser (общий для всех
                воркеров — экономия памяти, один процесс Chromium).

        Returns:
            Готовая к использованию страница Playwright.

        Raises:
            RuntimeError: Если не удалось создать контекст.
        """
        try:
            self._browser = browser
            self._owns_browser = False

            # Загружаем назначенные прокси (без чтения файла)
            self._load_proxies()

            # Берём первый прокси из назначенного пула
            first_proxy = self._get_next_proxy()

            self._context = await self._create_context(
                proxy=first_proxy,
            )

            self._page = await self._context.new_page()
            await self._page.add_init_script(STEALTH_SCRIPT)

            # Сбрасываем счётчик карточек
            self._listings_since_rotation = 0

            logger.info(
                "worker_context_launched",
                source=self._log_prefix(),
                proxy_enabled=len(self._proxies) > 0,
                proxy_count=len(self._proxies),
                proxy_server=(
                    first_proxy.server if first_proxy else "нет"
                ),
            )

            return self._page

        except Exception as e:
            logger.error(
                "worker_context_launch_failed",
                source=self._log_prefix(),
                exc_info=True,
                error=str(e),
            )
            await self.close()
            raise RuntimeError(
                f"Не удалось создать контекст воркера "
                f"{self._log_prefix()}: {e}"
            ) from e

    async def rotate_proxy(self) -> Page:
        """Переключается на следующий прокси из пула.

        Закрывает текущий контекст (cookies, session, fingerprint),
        берёт следующий прокси, создаёт полностью новый контекст
        с новым User-Agent, viewport и прокси. Chromium-процесс
        НЕ перезапускается — пересоздаётся только BrowserContext.

        Returns:
            Новая страница Playwright с чистым контекстом.

        Raises:
            RuntimeError: Если прокси не загружены или браузер не запущен.
        """
        if not self._proxies:
            raise RuntimeError(
                "Ротация прокси невозможна: прокси не загружены. "
                "Укажите PROXY_FILE_PATH в .env."
            )

        if self._browser is None:
            raise RuntimeError(
                "Ротация прокси невозможна: браузер не запущен."
            )

        old_proxy_index = self._current_proxy_index

        # Закрываем текущий контекст (чистим cookies, session, fingerprint)
        if self._context is not None:
            try:
                await self._context.close()
            except Exception as e:
                logger.warning(
                    "context_close_error_during_rotation",
                    source=self._log_prefix(),
                    error=str(e),
                )
            self._context = None
            self._page = None

        # Берём следующий прокси
        next_proxy = self._get_next_proxy()

        # Создаём полностью новый контекст
        self._context = await self._create_context(proxy=next_proxy)

        self._page = await self._context.new_page()
        await self._page.add_init_script(STEALTH_SCRIPT)

        # Сбрасываем счётчик карточек
        self._listings_since_rotation = 0

        logger.info(
            "proxy_rotated",
            source=self._log_prefix(),
            old_index=old_proxy_index,
            new_index=self._current_proxy_index,
            new_server=next_proxy.server if next_proxy else "нет",
            listings_before_rotation=self._listings_since_rotation,
        )

        # Пауза после смены прокси — имитация нового пользователя
        pause = random.uniform(3.0, 6.0)
        logger.debug(
            "post_rotation_pause",
            source=self._log_prefix(),
            pause=round(pause, 1),
        )
        await asyncio.sleep(pause)

        return self._page

    async def increment_and_check_rotation(self) -> Page | None:
        """Увеличивает счётчик карточек и проверяет необходимость ротации.

        Вызывается после каждой успешно обработанной карточки.
        Если счётчик достиг порога rotate_every_n — выполняет
        ротацию прокси. Если прокси не загружены или порог = 0 —
        ничего не делает.

        Returns:
            Новая Page после ротации, или None если ротация не нужна.
        """
        self._listings_since_rotation += 1

        # Ротация отключена: прокси не загружены или порог = 0
        if not self._proxies:
            return None
        if self._proxy_settings.rotate_every_n <= 0:
            return None

        # Порог не достигнут
        if self._listings_since_rotation < self._proxy_settings.rotate_every_n:
            return None

        logger.info(
            "rotation_threshold_reached",
            source=self._log_prefix(),
            listings_count=self._listings_since_rotation,
            threshold=self._proxy_settings.rotate_every_n,
        )

        new_page = await self.rotate_proxy()
        return new_page

    @property
    def has_proxies(self) -> bool:
        """Проверяет, загружены ли прокси.

        Returns:
            True если список прокси не пуст.
        """
        return len(self._proxies) > 0

    async def navigate(self, url: str) -> bool:
        """Переходит по URL с предварительной задержкой и проверкой блокировки.

        Имитирует поведение реального пользователя: случайная задержка
        перед переходом, ожидание CloudFlare challenge с повторными
        проверками, валидация загрузки контента Avito.

        При ошибке прокси-соединения (ERR_TUNNEL_CONNECTION_FAILED и др.)
        автоматически переключается на следующий прокси из пула и
        повторяет навигацию — до MAX_PROXY_RETRIES_ON_NAVIGATE попыток.

        Args:
            url: URL для перехода.

        Returns:
            True если страница загружена успешно и без блокировки.
        """
        if self._page is None:
            logger.error(
                "navigate_no_page",
                source=self._log_prefix(),
                url=url,
            )
            return False

        # Первая попытка навигации
        result = await self._try_navigate(url)
        if result is True:
            return True

        # result is False — обычная ошибка (блокировка, CloudFlare и т.д.)
        # result is None — ошибка прокси, нужна ротация
        if result is not None:
            return False

        # === Ошибка прокси → автоматический перебор ===
        if not self._proxies:
            logger.error(
                "proxy_error_no_proxies_to_retry",
                source=self._log_prefix(),
                url=url[:200],
            )
            return False

        max_retries = min(
            MAX_PROXY_RETRIES_ON_NAVIGATE, len(self._proxies)
        )

        for retry in range(1, max_retries + 1):
            logger.warning(
                "proxy_error_rotating",
                source=self._log_prefix(),
                url=url[:200],
                retry=retry,
                max_retries=max_retries,
            )
            print(
                f"  [{self._log_prefix()}][прокси] Прокси не работает. "
                f"Переключение на следующий "
                f"({retry}/{max_retries})..."
            )

            try:
                await self.rotate_proxy()
            except RuntimeError as e:
                logger.error(
                    "proxy_rotation_failed_during_navigate",
                    source=self._log_prefix(),
                    retry=retry,
                    error=str(e),
                )
                continue

            result = await self._try_navigate(url)
            if result is True:
                logger.info(
                    "navigation_success_after_proxy_retry",
                    source=self._log_prefix(),
                    url=url[:200],
                    retry=retry,
                )
                print(
                    f"  [{self._log_prefix()}][прокси] "
                    f"Прокси #{self._current_proxy_index} "
                    f"работает! Навигация успешна."
                )
                return True

            # result is False — страница загрузилась, но заблокирована
            if result is not None:
                logger.warning(
                    "navigation_blocked_after_proxy_retry",
                    source=self._log_prefix(),
                    url=url[:200],
                    retry=retry,
                )
                return False

            # result is None — снова ошибка прокси, пробуем следующий
            logger.debug(
                "proxy_still_failing",
                source=self._log_prefix(),
                retry=retry,
                proxy_index=self._current_proxy_index,
            )

        logger.error(
            "all_proxy_retries_exhausted_on_navigate",
            source=self._log_prefix(),
            url=url[:200],
            max_retries=max_retries,
        )
        print(
            f"  [{self._log_prefix()}][прокси] "
            f"Все попытки смены прокси исчерпаны "
            f"({max_retries} шт.). Навигация не удалась."
        )
        return False

    async def _try_navigate(self, url: str) -> bool | None:
        """Выполняет одну попытку навигации по URL.

        Разделяет результат на три случая:
        - True: страница загружена и доступна.
        - False: страница загрузилась, но заблокирована или
          CloudFlare не пройден (прокси работает, проблема в другом).
        - None: ошибка прокси-соединения (прокси мёртв,
          нужна ротация).

        Args:
            url: URL для перехода.

        Returns:
            True — успех, False — блокировка, None — ошибка прокси.
        """
        if self._page is None:
            logger.error(
                "try_navigate_no_page",
                source=self._log_prefix(),
                url=url[:200],
            )
            return False

        try:
            delay = random.uniform(2, 4)
            logger.info(
                "navigation_started",
                source=self._log_prefix(),
                url=url[:200],
                delay=round(delay, 1),
            )
            await asyncio.sleep(delay)

            await self._page.goto(
                url,
                wait_until="domcontentloaded",
                timeout=self._settings.navigation_timeout,
            )

            # Ожидание загрузки контента после первоначальной загрузки DOM
            await asyncio.sleep(8)

        except Exception as e:
            error_text = str(e)

            # Определяем: это ошибка прокси или что-то другое?
            if _is_proxy_error(error_text):
                logger.warning(
                    "navigation_proxy_error",
                    source=self._log_prefix(),
                    url=url[:200],
                    error=error_text[:300],
                    proxy_index=self._current_proxy_index,
                )
                return None

            logger.error(
                "navigation_failed",
                source=self._log_prefix(),
                url=url[:200],
                error=error_text[:300],
            )
            return False

        # Проверяем CloudFlare challenge с повторными попытками
        for attempt in range(1, MAX_CLOUDFLARE_RETRIES + 1):
            status = await self._check_page_status()

            if status == "ok":
                logger.info(
                    "navigation_success",
                    source=self._log_prefix(),
                    url=url[:200],
                    current_url=self._page.url[:200],
                )
                return True

            if status == "blocked":
                logger.warning(
                    "page_blocked",
                    source=self._log_prefix(),
                    url=url[:200],
                )
                return False

            if status == "cloudflare":
                logger.info(
                    "cloudflare_challenge_waiting",
                    source=self._log_prefix(),
                    attempt=attempt,
                    max_attempts=MAX_CLOUDFLARE_RETRIES,
                    wait_seconds=CLOUDFLARE_WAIT_SECONDS,
                )
                await asyncio.sleep(CLOUDFLARE_WAIT_SECONDS)

        # Если после всех попыток CloudFlare не прошёл —
        # проверяем, может контент всё же загрузился
        final_status = await self._check_page_status()
        if final_status == "ok":
            logger.info(
                "navigation_success_after_retries",
                source=self._log_prefix(),
                url=url[:200],
            )
            return True

        logger.warning(
            "cloudflare_challenge_failed",
            source=self._log_prefix(),
            url=url[:200],
            attempts=MAX_CLOUDFLARE_RETRIES,
        )
        return False

    async def _check_page_status(self) -> str:
        """Определяет текущий статус загруженной страницы.

        Анализирует заголовок и наличие элементов Avito, чтобы
        отличить успешную загрузку от блокировки или CloudFlare.

        Returns:
            Строка-статус:
            - "ok" — страница Avito загружена нормально
            - "blocked" — доступ заблокирован (403, Access Denied)
            - "cloudflare" — CloudFlare challenge ещё не пройден
            - "unknown" — не удалось определить статус
        """
        if self._page is None:
            return "blocked"

        try:
            title = await self._page.title()
            current_url = self._page.url

            logger.debug(
                "page_status_check",
                source=self._log_prefix(),
                title=title[:80],
                url=current_url[:100],
            )

            # Явная блокировка по заголовку
            blocked_titles = (
                "Доступ ограничен",
                "Access denied",
                "403 Forbidden",
                "Ошибка",
            )
            for phrase in blocked_titles:
                if phrase.lower() in title.lower():
                    return "blocked"

            # CloudFlare challenge ещё активен
            cloudflare_titles = (
                "Just a moment",
                "Checking your browser",
                "Проверка браузера",
            )
            for phrase in cloudflare_titles:
                if phrase.lower() in title.lower():
                    return "cloudflare"

            # Проверяем наличие реального контента Avito на странице.
            # Ищем характерные элементы: каталог товаров, шапка сайта,
            # поисковая строка. Если хотя бы один найден — страница ОК.
            avito_selectors = (
                "div[data-marker='catalog-serp']",
                "div[data-marker='search-form']",
                "a[data-marker='item-title']",
                "div[class*='index-root']",
                "input[data-marker='search-form/suggest']",
            )

            for selector in avito_selectors:
                element = await self._page.query_selector(selector)
                if element is not None:
                    logger.debug(
                        "avito_content_found",
                        source=self._log_prefix(),
                        selector=selector,
                    )
                    return "ok"

            # Если заголовок содержит "Avito" или "Авито" —
            # скорее всего страница загрузилась, просто нет товаров
            if "avito" in title.lower() or "авито" in title.lower():
                logger.debug(
                    "avito_title_found",
                    source=self._log_prefix(),
                    title=title[:80],
                )
                return "ok"

            # Ни блокировки, ни CloudFlare, ни контента Avito —
            # возможно страница ещё грузится
            return "unknown"

        except Exception as e:
            logger.warning(
                "page_status_check_failed",
                source=self._log_prefix(),
                error=str(e),
            )
            return "unknown"

    async def _check_blocked(self) -> bool:
        """Проверяет, заблокирован ли доступ к странице.

        Обёртка над _check_page_status() для обратной совместимости.

        Returns:
            True если обнаружена блокировка.
        """
        status = await self._check_page_status()
        return status == "blocked"

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

            logger.debug(
                "human_behavior_simulated",
                source=self._log_prefix(),
            )

        except Exception as e:
            logger.warning(
                "human_behavior_simulation_failed",
                source=self._log_prefix(),
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
        logger.debug(
            "page_wait_started",
            source=self._log_prefix(),
            wait_ms=wait_ms,
        )
        await self._page.wait_for_timeout(wait_ms)

    @property
    def page(self) -> Page | None:
        """Возвращает текущую активную страницу.

        Returns:
            Экземпляр Page или None если браузер не запущен.
        """
        return self._page

    async def close(self) -> None:
        """Закрывает контекст и (в автономном режиме) браузер.

        В режиме воркера закрывает только свой BrowserContext,
        не трогая Browser и Playwright — их жизненным циклом
        управляет ParallelListingService.

        В автономном режиме закрывает всё: контекст, браузер,
        Playwright.
        """
        try:
            if self._context is not None:
                await self._context.close()
                self._context = None

            # В режиме воркера не закрываем Browser и Playwright
            if self._owns_browser:
                if self._browser is not None:
                    await self._browser.close()
                    self._browser = None

                if self._playwright is not None:
                    await self._playwright.stop()  # type: ignore[union-attr]
                    self._playwright = None
            else:
                # Воркер: просто обнуляем ссылку, не закрываем
                self._browser = None

            self._page = None
            logger.info(
                "browser_closed",
                source=self._log_prefix(),
                owned=self._owns_browser,
            )

        except Exception as e:
            logger.warning(
                "browser_close_error",
                source=self._log_prefix(),
                error=str(e),
            )
