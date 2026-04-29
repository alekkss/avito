"""Сервис управления Playwright-браузером.

Инкапсулирует запуск браузера, создание контекста со stealth-настройками,
имитацию человеческого поведения, ротацию прокси и управление жизненным
циклом. Основан на шаблоне с антидетект-параметрами для обхода защиты Avito.

Поддерживает два режима работы:
- Автономный (launch): загружает прокси из файла, запускает свой Chromium.
- Воркер (launch_for_worker): использует переданный Browser-инстанс
  и назначенные прокси, не управляет жизненным циклом Playwright/Browser.

Антидетект-защита включает:
- Маскировку Canvas, WebGL, WebRTC, AudioContext fingerprint.
- Блокировку трекинговых скриптов, изображений, шрифтов, аналитики.
- Рандомизацию User-Agent, viewport, поведенческих паттернов.
- Прогрев сессии через обход нейтральных страниц Avito.

Интегрирован с ProxyHealthTracker для автоматического исключения
«мёртвых» прокси (забаненных Avito или не отвечающих) из пула ротации.

КЛЮЧЕВОЕ ИЗМЕНЕНИЕ: метод _get_next_proxy() теперь выбирает прокси
ТОЛЬКО из назначенного подпула воркера (_proxy_map), а не из
глобального пула трекера. Это устраняет ошибку proxy_not_found_in_map,
когда трекер возвращал прокси, принадлежащий другому воркеру.
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
    Route,
    async_playwright,
)

from src.config import BrowserSettings, ProxySettings, get_logger
from src.services.proxy_health import ProxyHealthTracker

logger = get_logger("browser_service")

# Список User-Agent для ротации при каждом запуске.
# Включает актуальные версии Chrome 120-125 для Windows, macOS, Linux.
USER_AGENTS: list[str] = [
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
]

# JavaScript для сокрытия признаков автоматизации.
# Покрывает: navigator.webdriver, chrome.runtime, plugins, languages,
# Canvas fingerprint, WebGL fingerprint, WebRTC leak prevention,
# AudioContext fingerprint, Permission API, NetworkInformation API.
STEALTH_SCRIPT: str = """
    // === 1. navigator.webdriver — базовая маскировка ===
    Object.defineProperty(navigator, 'webdriver', {
        get: () => false,
    });

    // === 2. chrome.runtime — имитация расширений ===
    window.chrome = {
        runtime: {
            onConnect: null,
            onMessage: null,
            sendMessage: function() {},
            connect: function() { return { onMessage: { addListener: function() {} } }; },
        },
        loadTimes: function() {
            return {
                requestTime: Date.now() / 1000 - Math.random() * 100,
                startLoadTime: Date.now() / 1000 - Math.random() * 50,
                commitLoadTime: Date.now() / 1000 - Math.random() * 10,
                finishDocumentLoadTime: Date.now() / 1000,
                finishLoadTime: Date.now() / 1000,
            };
        },
        csi: function() { return {}; },
    };

    // === 3. navigator.plugins — реалистичный набор плагинов ===
    Object.defineProperty(navigator, 'plugins', {
        get: () => {
            const plugins = [
                { name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer', description: 'Portable Document Format' },
                { name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai', description: '' },
                { name: 'Native Client', filename: 'internal-nacl-plugin', description: '' },
            ];
            plugins.length = 3;
            return plugins;
        },
    });

    // === 4. navigator.languages ===
    Object.defineProperty(navigator, 'languages', {
        get: () => ['ru-RU', 'ru', 'en-US', 'en'],
    });

    // === 5. navigator.hardwareConcurrency ===
    Object.defineProperty(navigator, 'hardwareConcurrency', {
        get: () => [4, 8, 12, 16][Math.floor(Math.random() * 4)],
    });

    // === 6. navigator.deviceMemory ===
    Object.defineProperty(navigator, 'deviceMemory', {
        get: () => [4, 8, 16][Math.floor(Math.random() * 3)],
    });

    // === 7. Canvas fingerprint — подмешивание шума ===
    // Avito использует canvas fingerprint для идентификации браузеров.
    // Подмешиваем случайный шум в toDataURL и toBlob, чтобы fingerprint
    // менялся при каждом создании контекста (но оставался стабильным
    // в пределах одной сессии — иначе детектируется как аномалия).
    (function() {
        const sessionNoise = Math.random() * 0.01;

        const originalToDataURL = HTMLCanvasElement.prototype.toDataURL;
        HTMLCanvasElement.prototype.toDataURL = function(type) {
            const context = this.getContext('2d');
            if (context && this.width > 0 && this.height > 0) {
                try {
                    const imageData = context.getImageData(0, 0, this.width, this.height);
                    const data = imageData.data;
                    // Модифицируем небольшую часть пикселей (1 из 100)
                    for (let i = 0; i < data.length; i += 400) {
                        data[i] = data[i] ^ (Math.floor(sessionNoise * 255) & 0xFF);
                    }
                    context.putImageData(imageData, 0, 0);
                } catch(e) {}
            }
            return originalToDataURL.apply(this, arguments);
        };

        const originalToBlob = HTMLCanvasElement.prototype.toBlob;
        HTMLCanvasElement.prototype.toBlob = function(callback, type, quality) {
            const context = this.getContext('2d');
            if (context && this.width > 0 && this.height > 0) {
                try {
                    const imageData = context.getImageData(0, 0, this.width, this.height);
                    const data = imageData.data;
                    for (let i = 0; i < data.length; i += 400) {
                        data[i] = data[i] ^ (Math.floor(sessionNoise * 255) & 0xFF);
                    }
                    context.putImageData(imageData, 0, 0);
                } catch(e) {}
            }
            return originalToBlob.apply(this, arguments);
        };
    })();

    // === 8. WebGL fingerprint — подмена renderer/vendor ===
    // Avito может запрашивать WebGL renderer для fingerprinting.
    // Подменяем на распространённые значения настольных GPU.
    (function() {
        const renderers = [
            'ANGLE (Intel(R) UHD Graphics 630 Direct3D11 vs_5_0 ps_5_0, D3D11)',
            'ANGLE (NVIDIA GeForce GTX 1060 Direct3D11 vs_5_0 ps_5_0, D3D11)',
            'ANGLE (AMD Radeon RX 580 Direct3D11 vs_5_0 ps_5_0, D3D11)',
            'ANGLE (Intel(R) Iris(R) Xe Graphics Direct3D11 vs_5_0 ps_5_0, D3D11)',
            'ANGLE (NVIDIA GeForce RTX 3060 Direct3D11 vs_5_0 ps_5_0, D3D11)',
        ];
        const chosenRenderer = renderers[Math.floor(Math.random() * renderers.length)];
        const chosenVendor = 'Google Inc. (Intel)';

        const getParameterProto = WebGLRenderingContext.prototype.getParameter;
        WebGLRenderingContext.prototype.getParameter = function(param) {
            const ext = this.getExtension('WEBGL_debug_renderer_info');
            if (ext) {
                if (param === ext.UNMASKED_VENDOR_WEBGL) return chosenVendor;
                if (param === ext.UNMASKED_RENDERER_WEBGL) return chosenRenderer;
            }
            return getParameterProto.apply(this, arguments);
        };

        if (typeof WebGL2RenderingContext !== 'undefined') {
            const getParameter2Proto = WebGL2RenderingContext.prototype.getParameter;
            WebGL2RenderingContext.prototype.getParameter = function(param) {
                const ext = this.getExtension('WEBGL_debug_renderer_info');
                if (ext) {
                    if (param === ext.UNMASKED_VENDOR_WEBGL) return chosenVendor;
                    if (param === ext.UNMASKED_RENDERER_WEBGL) return chosenRenderer;
                }
                return getParameter2Proto.apply(this, arguments);
            };
        }
    })();

    // === 9. WebRTC — блокировка утечки реального IP ===
    // WebRTC может выдать реальный IP даже при использовании прокси.
    // Переопределяем RTCPeerConnection, чтобы предотвратить это.
    (function() {
        const originalRTCPeerConnection = window.RTCPeerConnection ||
            window.webkitRTCPeerConnection || window.mozRTCPeerConnection;

        if (originalRTCPeerConnection) {
            const handler = {
                construct(target, args) {
                    if (args[0] && args[0].iceServers) {
                        args[0].iceServers = [];
                    }
                    const instance = new target(...args);
                    const origCreateOffer = instance.createOffer.bind(instance);
                    instance.createOffer = function(options) {
                        if (options) {
                            options.offerToReceiveAudio = false;
                            options.offerToReceiveVideo = false;
                        }
                        return origCreateOffer(options);
                    };
                    return instance;
                }
            };
            window.RTCPeerConnection = new Proxy(originalRTCPeerConnection, handler);
            if (window.webkitRTCPeerConnection) {
                window.webkitRTCPeerConnection = window.RTCPeerConnection;
            }
        }
    })();

    // === 10. AudioContext fingerprint — шум в getChannelData ===
    // AudioContext fingerprinting анализирует особенности аудио-обработки.
    // Подмешиваем шум, чтобы fingerprint менялся между сессиями.
    (function() {
        const audioNoise = Math.random() * 0.0001;

        if (typeof AudioBuffer !== 'undefined') {
            const originalGetChannelData = AudioBuffer.prototype.getChannelData;
            AudioBuffer.prototype.getChannelData = function(channel) {
                const data = originalGetChannelData.apply(this, arguments);
                // Модифицируем небольшую часть сэмплов
                for (let i = 0; i < data.length; i += 100) {
                    data[i] = data[i] + audioNoise;
                }
                return data;
            };
        }
    })();

    // === 11. Permission API — маскировка статуса notifications ===
    if (typeof navigator.permissions !== 'undefined') {
        const originalQuery = navigator.permissions.query;
        navigator.permissions.query = function(parameters) {
            if (parameters.name === 'notifications') {
                return Promise.resolve({ state: Notification.permission });
            }
            return originalQuery.apply(this, arguments);
        };
    }

    // === 12. navigator.connection (NetworkInformation API) ===
    if (!navigator.connection) {
        Object.defineProperty(navigator, 'connection', {
            get: () => ({
                effectiveType: '4g',
                rtt: 50,
                downlink: 10,
                saveData: false,
            }),
        });
    }

    // === 13. window.Notification — скрываем отсутствие ===
    if (typeof Notification === 'undefined') {
        window.Notification = {
            permission: 'default',
            requestPermission: function() {
                return Promise.resolve('default');
            },
        };
    }

    // === 14. iframe contentWindow — маскировка ===
    // Некоторые скрипты проверяют, является ли iframe автоматизированным.
    try {
        const originalContentWindow = Object.getOwnPropertyDescriptor(
            HTMLIFrameElement.prototype, 'contentWindow'
        );
        if (originalContentWindow) {
            Object.defineProperty(HTMLIFrameElement.prototype, 'contentWindow', {
                get: function() {
                    const win = originalContentWindow.get.call(this);
                    if (win) {
                        try {
                            Object.defineProperty(win.navigator, 'webdriver', {
                                get: () => false,
                            });
                        } catch(e) {}
                    }
                    return win;
                },
            });
        }
    } catch(e) {}
"""

# Паттерны URL для блокировки ненужных ресурсов.
# Блокируем: трекинговые скрипты, аналитику, рекламу, шрифты, изображения.
# Это экономит трафик прокси, ускоряет загрузку и снижает
# fingerprinting-поверхность (трекеры — основной источник детектирования).
BLOCKED_RESOURCE_PATTERNS: list[str] = [
    # Аналитика и трекинг
    "mc.yandex.ru",
    "yandex.ru/metrika",
    "google-analytics.com",
    "googletagmanager.com",
    "google.com/pagead",
    "doubleclick.net",
    "googlesyndication.com",
    "top-fwz1.mail.ru",
    "top.mail.ru",
    "counter.yadro.ru",
    "vk.com/rtrg",
    "ads.adfox.ru",
    "an.yandex.ru",
    "ad.mail.ru",
    "sentry.io",
    "sentry-cdn.com",
    # Шрифты (снижают уникальность fingerprint)
    "fonts.googleapis.com",
    "fonts.gstatic.com",
    # Facebook/Meta пиксель
    "connect.facebook.net",
    "facebook.com/tr",
    # Другие трекеры
    "hotjar.com",
    "clarity.ms",
    "amplitude.com",
    "mixpanel.com",
    "segment.io",
    "segment.com",
]

# Типы ресурсов Playwright для блокировки.
# Изображения и шрифты не нужны для парсинга данных.
BLOCKED_RESOURCE_TYPES: set[str] = {
    "image",
    "font",
    "media",
}

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
    # Дополнительные аргументы для усиления антидетекта
    "--disable-features=WebRtcHideLocalIpsWithMdns",
    "--disable-webrtc-hw-encoding",
    "--disable-webrtc-hw-decoding",
    "--enforce-webrtc-ip-permission-check",
    "--force-webrtc-ip-handling-policy=disable_non_proxied_udp",
    "--disable-reading-from-canvas",
    "--disable-component-update",
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

# Маркеры ошибки разрушенного контекста/страницы Playwright.
# Когда прокси-соединение обрывается, Playwright может закрыть
# контекст — все последующие вызовы page.goto() будут падать с
# этой ошибкой. Нужно пересоздать контекст, а не пытаться
# использовать мёртвую page.
CONTEXT_DEAD_MARKERS: tuple[str, ...] = (
    "Target page, context or browser has been closed",
    "has been closed",
    "Target closed",
    "Context closed",
    "Browser closed",
)

# Максимальное количество смен прокси при навигации на каталог
MAX_PROXY_RETRIES_ON_NAVIGATE: int = 10

# URL для прогрева сессии — нейтральные страницы Avito,
# которые не вызовут подозрений и создадут легитимную историю cookies
WARMUP_URLS: list[str] = [
    "https://www.avito.ru",
    "https://www.avito.ru/sankt-peterburg",
    "https://www.avito.ru/moskva",
    "https://www.avito.ru/ekaterinburg",
    "https://www.avito.ru/novosibirsk",
    "https://www.avito.ru/kazan",
]


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


def _is_context_dead_error(error_text: str) -> bool:
    """Проверяет, указывает ли ошибка на разрушенный контекст/страницу.

    Когда прокси-соединение внезапно обрывается, Playwright может
    закрыть BrowserContext. Все последующие вызовы page.goto() будут
    падать с ошибкой «Target page, context or browser has been closed».
    В этом случае нужно пересоздать контекст, а не пытаться
    использовать мёртвую page.

    Args:
        error_text: Текст ошибки (str(exception)).

    Returns:
        True если ошибка указывает на разрушенный контекст.
    """
    return any(marker in error_text for marker in CONTEXT_DEAD_MARKERS)


def _should_block_request(url: str, resource_type: str) -> bool:
    """Проверяет, нужно ли заблокировать запрос.

    Блокирует запросы к трекинговым скриптам, аналитике,
    изображениям, шрифтам и другим ненужным ресурсам.
    Это экономит трафик прокси, ускоряет загрузку
    и снижает fingerprinting-поверхность.

    Args:
        url: URL запроса.
        resource_type: Тип ресурса Playwright (image, font и т.д.).

    Returns:
        True если запрос нужно заблокировать.
    """
    # Блокируем по типу ресурса
    if resource_type in BLOCKED_RESOURCE_TYPES:
        return True

    # Блокируем по паттернам URL
    url_lower = url.lower()
    for pattern in BLOCKED_RESOURCE_PATTERNS:
        if pattern in url_lower:
            return True

    return False


class BrowserService:
    """Сервис для управления Playwright-браузером со stealth-режимом.

    Инкапсулирует всю логику работы с браузером: запуск, настройку
    контекста, stealth-инъекции, ротацию прокси и имитацию поведения
    пользователя.

    Антидетект-защита включает:
    - Маскировку Canvas, WebGL, WebRTC, AudioContext fingerprint.
    - Блокировку трекинговых скриптов, изображений, шрифтов.
    - Рандомизацию User-Agent, viewport, поведенческих паттернов.
    - Прогрев сессии через обход нейтральных страниц Avito.

    Интегрирован с ProxyHealthTracker: при каждой навигации сообщает
    трекеру о результате (успех, бан, ошибка соединения). Трекер
    автоматически исключает «мёртвые» прокси из пула, что ускоряет
    ротацию и снижает количество бесполезных попыток.

    КЛЮЧЕВОЕ ИЗМЕНЕНИЕ (v2): _get_next_proxy() теперь выбирает прокси
    ТОЛЬКО из своего подпула (_proxy_map). Раньше он использовал
    health_tracker.get_next_healthy(), который возвращал сервер из
    глобального пула всех воркеров → ошибка proxy_not_found_in_map.
    Теперь: перебирает _proxies циклически, проверяя статус через
    трекер, и возвращает первый здоровый из СВОЕГО набора.

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
        _proxy_map: Словарь server → ProxyInfo для быстрого поиска.
        _current_proxy_server: URL текущего активного прокси (или "").
        _current_proxy_index: Индекс текущего прокси в _proxies
            для циклического перебора внутри своего подпула.
        _listings_since_rotation: Счётчик карточек с последней ротации.
        _owns_browser: True если этот экземпляр управляет жизненным
            циклом Playwright и Browser (автономный режим).
        _worker_id: Идентификатор воркера для логирования (None если
            автономный режим).
        _health_tracker: Трекер здоровья прокси.
    """

    def __init__(
        self,
        settings: BrowserSettings,
        proxy_settings: ProxySettings,
        assigned_proxies: list[ProxyInfo] | None = None,
        worker_id: int | None = None,
        health_tracker: ProxyHealthTracker | None = None,
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
            health_tracker: Трекер здоровья прокси. Если None —
                создаётся внутренний экземпляр (обратная совместимость).
        """
        self._settings = settings
        self._proxy_settings = proxy_settings
        self._playwright: Playwright | None = None
        self._browser: Browser | None = None
        self._context: BrowserContext | None = None
        self._page: Page | None = None
        self._assigned_proxies = assigned_proxies
        self._proxies: list[ProxyInfo] = []
        self._proxy_map: dict[str, ProxyInfo] = {}
        self._current_proxy_server: str = ""
        self._current_proxy_index: int = -1
        self._listings_since_rotation: int = 0
        self._owns_browser: bool = True
        self._worker_id = worker_id
        self._health_tracker = (
            health_tracker
            if health_tracker is not None
            else ProxyHealthTracker()
        )

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

        После загрузки регистрирует все прокси в health_tracker
        и строит словарь server → ProxyInfo для быстрого поиска.

        Если в конструктор переданы assigned_proxies — использует их.
        Иначе загружает из файла по пути из настроек.
        При пустом пути — парсер работает без прокси.
        """
        # Если прокси назначены извне (режим воркера) — используем их
        if self._assigned_proxies is not None:
            self._proxies = list(self._assigned_proxies)
        elif self._proxy_settings.proxy_file_path:
            # Автономный режим — загружаем из файла
            self._proxies = load_proxies_from_file(
                self._proxy_settings.proxy_file_path
            )
            # Перемешиваем для равномерного распределения нагрузки
            random.shuffle(self._proxies)
        else:
            logger.info(
                "proxy_disabled_no_file",
                source=self._log_prefix(),
            )
            return

        # Строим словарь server → ProxyInfo для быстрого поиска
        self._proxy_map = {p.server: p for p in self._proxies}

        # Регистрируем в трекере здоровья
        self._health_tracker.register_many(
            [p.server for p in self._proxies]
        )

        logger.info(
            "proxies_ready",
            source=self._log_prefix(),
            total=len(self._proxies),
            rotate_every=self._proxy_settings.rotate_every_n,
        )

    def _get_next_proxy(self) -> ProxyInfo | None:
        """Возвращает следующий здоровый прокси из СВОЕГО подпула.

        КЛЮЧЕВОЕ ИЗМЕНЕНИЕ (v2): перебирает прокси ТОЛЬКО из
        _proxies (назначенный подпул воркера), а НЕ из глобального
        пула трекера. Для каждого кандидата проверяет статус через
        health_tracker.is_dead(). Возвращает первый здоровый.

        Это устраняет ошибку proxy_not_found_in_map: раньше
        health_tracker.get_next_healthy() мог вернуть сервер,
        принадлежащий другому воркеру, и _proxy_map.get() давал None.

        Перебор циклический: начинает с позиции после текущего прокси,
        проходит весь пул за один оборот. Если все прокси мертвы —
        возвращает None.

        Returns:
            Следующий здоровый ProxyInfo из своего подпула или None.
        """
        if not self._proxies:
            return None

        pool_size = len(self._proxies)

        # Перебираем весь свой подпул начиная со следующего индекса
        for offset in range(1, pool_size + 1):
            candidate_index = (
                (self._current_proxy_index + offset) % pool_size
            )
            candidate = self._proxies[candidate_index]

            # Проверяем здоровье через общий трекер
            if self._health_tracker.is_dead(candidate.server):
                continue

            # Нашли здоровый прокси — обновляем индекс
            self._current_proxy_index = candidate_index

            logger.info(
                "proxy_selected",
                source=self._log_prefix(),
                server=candidate.server,
                pool_index=candidate_index,
                pool_size=pool_size,
                alive_in_pool=self._count_alive_in_pool(),
                alive_global=self._health_tracker.alive_count,
                total_global=self._health_tracker.total_count,
            )

            return candidate

        # Все прокси в нашем подпуле мертвы
        logger.error(
            "no_healthy_proxies_in_pool",
            source=self._log_prefix(),
            pool_size=pool_size,
            alive_global=self._health_tracker.alive_count,
            total_global=self._health_tracker.total_count,
        )
        return None

    def _count_alive_in_pool(self) -> int:
        """Считает количество живых прокси в своём подпуле.

        Returns:
            Количество прокси со статусом != DEAD в _proxies.
        """
        return sum(
            1 for p in self._proxies
            if not self._health_tracker.is_dead(p.server)
        )

    @property
    def current_proxy_server(self) -> str:
        """URL текущего активного прокси.

        Returns:
            URL прокси-сервера или пустая строка если без прокси.
        """
        return self._current_proxy_server

    @property
    def health_tracker(self) -> ProxyHealthTracker:
        """Возвращает трекер здоровья прокси.

        Используется для финального отчёта и для передачи
        в другие сервисы (ParallelListingService).

        Returns:
            Экземпляр ProxyHealthTracker.
        """
        return self._health_tracker

    @property
    def is_context_alive(self) -> bool:
        """Проверяет, жив ли текущий BrowserContext и Page.

        Используется для раннего обнаружения разрушенного
        контекста (после обрыва прокси-соединения) без попытки
        навигации. Позволяет listing_service быстро инициировать
        пересоздание контекста вместо бесполезных retry.

        Returns:
            True если контекст и страница существуют и не закрыты.
        """
        if self._context is None or self._page is None:
            return False

        try:
            # Проверяем, что page не закрыта
            return not self._page.is_closed()
        except Exception:
            return False

    def report_success(self) -> None:
        """Сообщает трекеру об успешном использовании текущего прокси.

        Вызывается из ListingService после успешного парсинга
        карточки — сбрасывает серию банов текущего прокси.
        """
        if self._current_proxy_server:
            self._health_tracker.report_success(
                self._current_proxy_server
            )

    def report_ban(self) -> None:
        """Сообщает трекеру о бане текущего прокси.

        Вызывается из ListingService при обнаружении блокировки
        на странице карточки.
        """
        if self._current_proxy_server:
            self._health_tracker.report_ban(
                self._current_proxy_server
            )
            logger.info(
                "ban_reported_to_tracker",
                source=self._log_prefix(),
                server=self._current_proxy_server,
                is_dead=self._health_tracker.is_dead(
                    self._current_proxy_server
                ),
            )

    async def _setup_resource_blocking(self, page: Page) -> None:
        """Настраивает блокировку ненужных ресурсов через route.

        Перехватывает все сетевые запросы страницы и блокирует
        те, которые соответствуют паттернам трекинговых скриптов,
        аналитики, изображений и шрифтов. Это:
        - Экономит трафик прокси (до 70% снижение).
        - Ускоряет загрузку страниц (в 2-3 раза).
        - Снижает fingerprinting-поверхность (трекинговые скрипты —
          основной источник детектирования ботов).

        Args:
            page: Страница Playwright для настройки.
        """
        async def route_handler(route: Route) -> None:
            """Обработчик перехвата запросов."""
            request = route.request
            url = request.url
            resource_type = request.resource_type

            if _should_block_request(url, resource_type):
                await route.abort()
                return

            await route.continue_()

        try:
            await page.route("**/*", route_handler)
            logger.debug(
                "resource_blocking_enabled",
                source=self._log_prefix(),
                blocked_patterns=len(BLOCKED_RESOURCE_PATTERNS),
                blocked_types=len(BLOCKED_RESOURCE_TYPES),
            )
        except Exception as e:
            logger.warning(
                "resource_blocking_setup_failed",
                source=self._log_prefix(),
                error=str(e),
            )

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

    async def _create_page_with_stealth(
        self, context: BrowserContext
    ) -> Page:
        """Создаёт страницу с полной stealth-инъекцией и блокировкой ресурсов.

        Выделен в отдельный метод, чтобы гарантировать единообразную
        настройку страниц во всех режимах (launch, launch_for_worker,
        rotate_proxy).

        Args:
            context: Контекст браузера.

        Returns:
            Страница с инъецированным stealth-скриптом и блокировкой.
        """
        page = await context.new_page()
        await page.add_init_script(STEALTH_SCRIPT)
        await self._setup_resource_blocking(page)
        return page

    async def launch(self) -> Page:
        """Запускает браузер, создаёт контекст и страницу.

        Автономный режим: загрузка прокси из файла,
        запуск Chromium с антидетект-аргументами, создание контекста
        с первым прокси (или без прокси), инъекция stealth-скриптов,
        настройка блокировки ненужных ресурсов.

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

            # Берём первый здоровый прокси (или None)
            first_proxy = self._get_next_proxy()
            self._current_proxy_server = (
                first_proxy.server if first_proxy else ""
            )

            self._context = await self._create_context(
                proxy=first_proxy,
            )

            self._page = await self._create_page_with_stealth(
                self._context
            )

            # Сбрасываем счётчик карточек
            self._listings_since_rotation = 0

            logger.info(
                "browser_launched",
                source=self._log_prefix(),
                headless=self._settings.headless,
                timeout=self._settings.navigation_timeout,
                proxy_enabled=len(self._proxies) > 0,
                proxy_server=self._current_proxy_server or "нет",
                resource_blocking="enabled",
                stealth="extended",
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

            # Берём первый здоровый прокси из назначенного пула
            first_proxy = self._get_next_proxy()
            self._current_proxy_server = (
                first_proxy.server if first_proxy else ""
            )

            self._context = await self._create_context(
                proxy=first_proxy,
            )

            self._page = await self._create_page_with_stealth(
                self._context
            )

            # Сбрасываем счётчик карточек
            self._listings_since_rotation = 0

            logger.info(
                "worker_context_launched",
                source=self._log_prefix(),
                proxy_enabled=len(self._proxies) > 0,
                proxy_count=len(self._proxies),
                proxy_server=self._current_proxy_server or "нет",
                resource_blocking="enabled",
                stealth="extended",
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

    async def recreate_context(self) -> Page:
        """Пересоздаёт BrowserContext на текущем прокси.

        Используется при обнаружении ошибки «Target page, context or
        browser has been closed». В этом случае контекст разрушен
        (Playwright закрыл его из-за обрыва прокси-соединения), но
        сам Browser жив. Создаём новый контекст на том же прокси —
        прокси мог временно потерять соединение, но не быть забаненным.

        Если текущий прокси мёртв (по данным трекера) — выбирает
        следующий здоровый из своего подпула.

        Returns:
            Новая страница Playwright с чистым контекстом.

        Raises:
            RuntimeError: Если браузер не запущен или нет живых прокси.
        """
        if self._browser is None:
            raise RuntimeError(
                "Пересоздание контекста невозможно: браузер не запущен."
            )

        # Пытаемся закрыть старый контекст (может быть уже мёртв)
        if self._context is not None:
            try:
                await self._context.close()
            except Exception:
                pass
            self._context = None
            self._page = None

        # Определяем, какой прокси использовать
        proxy_to_use: ProxyInfo | None = None

        if self._current_proxy_server:
            # Текущий прокси ещё жив? Используем его же.
            if not self._health_tracker.is_dead(self._current_proxy_server):
                proxy_to_use = self._proxy_map.get(
                    self._current_proxy_server
                )
            else:
                # Текущий мёртв — берём следующий здоровый
                proxy_to_use = self._get_next_proxy()
                if proxy_to_use is None:
                    raise RuntimeError(
                        "Пересоздание контекста невозможно: "
                        "все прокси в подпуле мертвы."
                    )
        else:
            # Без прокси — просто пересоздаём контекст
            proxy_to_use = None

        self._current_proxy_server = (
            proxy_to_use.server if proxy_to_use else ""
        )

        # Создаём новый контекст
        self._context = await self._create_context(proxy=proxy_to_use)
        self._page = await self._create_page_with_stealth(self._context)

        # Сбрасываем счётчик
        self._listings_since_rotation = 0

        logger.info(
            "context_recreated",
            source=self._log_prefix(),
            proxy_server=self._current_proxy_server or "без прокси",
            reason="контекст был разрушен (Target page closed)",
        )

        # Небольшая пауза после пересоздания
        await asyncio.sleep(random.uniform(2.0, 4.0))

        return self._page

    async def rotate_proxy(self) -> Page:
        """Переключается на следующий здоровый прокси из подпула.

        Закрывает текущий контекст (cookies, session, fingerprint),
        берёт следующий здоровый прокси из СВОЕГО подпула через
        _get_next_proxy(), создаёт полностью новый контекст с новым
        User-Agent, viewport и прокси. Chromium-процесс НЕ перезапускается.

        Если все прокси в подпуле мертвы — выбрасывает RuntimeError.

        Returns:
            Новая страница Playwright с чистым контекстом.

        Raises:
            RuntimeError: Если нет здоровых прокси или браузер не запущен.
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

        old_proxy_server = self._current_proxy_server

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

        # Берём следующий здоровый прокси ИЗ СВОЕГО ПОДПУЛА
        next_proxy = self._get_next_proxy()

        if next_proxy is None:
            raise RuntimeError(
                "Ротация прокси невозможна: все прокси исключены "
                "из пула (DEAD). Добавьте новые прокси в "
                "proxies.txt."
            )

        self._current_proxy_server = next_proxy.server

        # Создаём полностью новый контекст
        self._context = await self._create_context(proxy=next_proxy)

        self._page = await self._create_page_with_stealth(
            self._context
        )

        # Сбрасываем счётчик карточек
        self._listings_since_rotation = 0

        logger.info(
            "proxy_rotated",
            source=self._log_prefix(),
            old_server=old_proxy_server,
            new_server=next_proxy.server,
            alive_in_pool=self._count_alive_in_pool(),
            alive_global=self._health_tracker.alive_count,
            total_global=self._health_tracker.total_count,
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
        """Проверяет, есть ли здоровые прокси в подпуле воркера.

        ИЗМЕНЕНО (v2): проверяет именно свой подпул (_proxies),
        а не глобальный alive_count трекера. Это корректнее:
        у другого воркера могут быть живые прокси, но данный
        воркер не может их использовать.

        Returns:
            True если есть хотя бы один живой прокси в подпуле.
        """
        if not self._proxies:
            return False
        return self._count_alive_in_pool() > 0

    async def navigate(self, url: str) -> bool:
        """Переходит по URL с предварительной задержкой и проверкой блокировки.

        Имитирует поведение реального пользователя: случайная задержка
        перед переходом, ожидание CloudFlare challenge с повторными
        проверками, валидация загрузки контента Avito.

        При ошибке прокси-соединения (ERR_TUNNEL_CONNECTION_FAILED и др.)
        сообщает трекеру об ошибке и автоматически переключается
        на следующий здоровый прокси. При бане — сообщает трекеру о бане.
        При успехе — сообщает трекеру об успехе.

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
            # Успех — сообщаем трекеру
            if self._current_proxy_server:
                self._health_tracker.report_success(
                    self._current_proxy_server
                )
            return True

        # result is False — блокировка/бан (прокси работает, но забанен)
        if result is not None:
            if self._current_proxy_server:
                self._health_tracker.report_ban(
                    self._current_proxy_server
                )
            return False

        # result is None — ошибка прокси-соединения
        if self._current_proxy_server:
            self._health_tracker.report_connection_error(
                self._current_proxy_server
            )

        # === Ошибка прокси → автоматический перебор здоровых ===
        if not self._proxies:
            logger.error(
                "proxy_error_no_proxies_to_retry",
                source=self._log_prefix(),
                url=url[:200],
            )
            return False

        alive_count = self._count_alive_in_pool()
        max_retries = min(MAX_PROXY_RETRIES_ON_NAVIGATE, alive_count)

        if max_retries <= 0:
            logger.error(
                "no_healthy_proxies_for_retry",
                source=self._log_prefix(),
                url=url[:200],
                pool_size=len(self._proxies),
            )
            return False

        for retry in range(1, max_retries + 1):
            logger.warning(
                "proxy_error_rotating",
                source=self._log_prefix(),
                url=url[:200],
                retry=retry,
                max_retries=max_retries,
                alive_in_pool=self._count_alive_in_pool(),
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
                return False

            result = await self._try_navigate(url)
            if result is True:
                # Успех с новым прокси
                if self._current_proxy_server:
                    self._health_tracker.report_success(
                        self._current_proxy_server
                    )

                logger.info(
                    "navigation_success_after_proxy_retry",
                    source=self._log_prefix(),
                    url=url[:200],
                    retry=retry,
                )
                print(
                    f"  [{self._log_prefix()}][прокси] "
                    f"Прокси {self._current_proxy_server} "
                    f"работает! Навигация успешна."
                )
                return True

            # result is False — страница загрузилась, но заблокирована
            if result is not None:
                if self._current_proxy_server:
                    self._health_tracker.report_ban(
                        self._current_proxy_server
                    )
                logger.warning(
                    "navigation_blocked_after_proxy_retry",
                    source=self._log_prefix(),
                    url=url[:200],
                    retry=retry,
                )
                return False

            # result is None — снова ошибка прокси-соединения
            if self._current_proxy_server:
                self._health_tracker.report_connection_error(
                    self._current_proxy_server
                )

            logger.debug(
                "proxy_still_failing",
                source=self._log_prefix(),
                retry=retry,
                server=self._current_proxy_server,
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

            # Определяем: это ошибка разрушенного контекста?
            if _is_context_dead_error(error_text):
                logger.warning(
                    "navigation_context_dead",
                    source=self._log_prefix(),
                    url=url[:200],
                    error=error_text[:200],
                    server=self._current_proxy_server,
                )
                # Возвращаем None — это эквивалент ошибки прокси:
                # контекст уничтожен, нужна ротация/пересоздание
                return None

            # Определяем: это ошибка прокси или что-то другое?
            if _is_proxy_error(error_text):
                logger.warning(
                    "navigation_proxy_error",
                    source=self._log_prefix(),
                    url=url[:200],
                    error=error_text[:300],
                    server=self._current_proxy_server,
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
            # скорее всего страница загрузилась
            if "avito" in title.lower() or "авито" in title.lower():
                logger.debug(
                    "avito_title_found",
                    source=self._log_prefix(),
                    title=title[:80],
                )
                return "ok"

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

        Выполняет случайные движения мыши по плавным кривым,
        прокрутку страницы с переменной скоростью и случайные
        паузы. Более реалистичное поведение по сравнению с
        прямолинейными движениями — снижает вероятность
        обнаружения автоматизации.
        """
        if self._page is None:
            return

        try:
            # Фаза 1: Плавные движения мыши по кривой (не прямые линии)
            # Имитируем человеческое движение — с «дрожанием» и паузами
            current_x = random.randint(200, 600)
            current_y = random.randint(150, 400)
            await self._page.mouse.move(current_x, current_y)
            await asyncio.sleep(random.uniform(0.3, 0.8))

            for _ in range(random.randint(2, 4)):
                # Целевая точка
                target_x = random.randint(100, 900)
                target_y = random.randint(100, 700)

                # Промежуточные точки для плавности (кривая Безье)
                mid_x = (current_x + target_x) // 2 + random.randint(-100, 100)
                mid_y = (current_y + target_y) // 2 + random.randint(-80, 80)

                # Двигаемся через промежуточную точку
                await self._page.mouse.move(mid_x, mid_y)
                await asyncio.sleep(random.uniform(0.05, 0.15))
                await self._page.mouse.move(target_x, target_y)
                await asyncio.sleep(random.uniform(0.3, 1.0))

                current_x = target_x
                current_y = target_y

            # Фаза 2: Прокрутка страницы с переменной скоростью
            # Человек прокручивает рывками, а не плавно
            scroll_steps = random.randint(2, 4)
            for step in range(scroll_steps):
                scroll_amount = random.randint(150, 500)
                await self._page.evaluate(
                    f"window.scrollBy(0, {scroll_amount})"
                )
                await asyncio.sleep(random.uniform(0.5, 1.5))

            # Фаза 3: Пауза «чтения» — человек останавливается
            await asyncio.sleep(random.uniform(1.0, 3.0))

            # Фаза 4: Прокрутка обратно наверх (не всегда)
            if random.random() < 0.6:
                await self._page.evaluate("window.scrollTo(0, 0)")
                await asyncio.sleep(random.uniform(0.5, 1.5))

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

    async def warmup_session(self) -> None:
        """Прогревает сессию через обход нейтральных страниц Avito.

        Перед началом парсинга карточек заходит на 1-2 случайные
        нейтральные страницы Avito (главная, города), имитирует
        просмотр и накапливает cookies. Это создаёт «легитимную»
        историю браузера и снижает вероятность бана на первой
        карточке.

        Безопасен для вызова — при ошибке просто логирует
        предупреждение и не прерывает работу.
        """
        if self._page is None:
            return

        # Выбираем 1-2 случайные URL для прогрева
        warmup_count = random.randint(1, 2)
        urls = random.sample(
            WARMUP_URLS,
            min(warmup_count, len(WARMUP_URLS)),
        )

        for url in urls:
            try:
                logger.info(
                    "warmup_navigating",
                    source=self._log_prefix(),
                    url=url,
                )

                await self._page.goto(
                    url,
                    wait_until="domcontentloaded",
                    timeout=self._settings.navigation_timeout,
                )
                await asyncio.sleep(random.uniform(3.0, 6.0))

                # Имитируем просмотр страницы
                await self.simulate_human_behavior()

                logger.info(
                    "warmup_page_visited",
                    source=self._log_prefix(),
                    url=url,
                )

            except Exception as e:
                logger.warning(
                    "warmup_page_failed",
                    source=self._log_prefix(),
                    url=url,
                    error=str(e),
                )

        # Пауза между прогревом и началом парсинга
        pause = random.uniform(2.0, 4.0)
        await asyncio.sleep(pause)

        logger.info(
            "warmup_completed",
            source=self._log_prefix(),
            pages_visited=len(urls),
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
