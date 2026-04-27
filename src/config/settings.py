"""Модуль конфигурации приложения.

Загружает переменные окружения из .env файла, валидирует обязательные
параметры и предоставляет единый объект Settings для доступа ко всем
настройкам приложения.
"""

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

# Корень проекта — два уровня вверх от этого файла (src/config/settings.py)
PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent.parent


def _load_env() -> None:
    """Загружает переменные окружения из .env файла.

    Ищет .env файл в корне проекта.
    Если файл не найден, переменные берутся из системного окружения.
    """
    env_path = PROJECT_ROOT / ".env"
    load_dotenv(dotenv_path=env_path)


def _resolve_path(raw_path: str) -> str:
    """Преобразует относительный путь в абсолютный относительно корня проекта.

    Если путь уже абсолютный — возвращает как есть.
    Если путь пустой — возвращает пустую строку.
    Это гарантирует, что файлы (БД, логи, Excel, прокси) всегда
    создаются внутри папки проекта, независимо от рабочей директории
    процесса (cwd).

    Args:
        raw_path: Исходный путь из переменной окружения.

    Returns:
        Абсолютный путь в виде строки.
    """
    if not raw_path:
        return raw_path
    path = Path(raw_path)
    if path.is_absolute():
        return raw_path
    return str(PROJECT_ROOT / path)


class ConfigValidationError(Exception):
    """Ошибка валидации конфигурации.

    Выбрасывается при отсутствии обязательных переменных окружения
    или при некорректных значениях параметров.
    """


@dataclass(frozen=True)
class BrowserSettings:
    """Настройки Playwright-браузера.

    Attributes:
        headless: Запуск без графического интерфейса.
        navigation_timeout: Таймаут навигации в миллисекундах.
        page_wait_time: Время ожидания после загрузки страницы (мс).
    """

    headless: bool
    navigation_timeout: int
    page_wait_time: int


@dataclass(frozen=True)
class ScraperSettings:
    """Настройки парсинга Avito.

    Attributes:
        category_url: URL категории Avito с фильтрами.
        max_pages: Максимальное число страниц пагинации (0 = все).
    """

    category_url: str
    max_pages: int


@dataclass(frozen=True)
class DatabaseSettings:
    """Настройки базы данных SQLite.

    Attributes:
        db_path: Путь к файлу базы данных.
    """

    db_path: str


@dataclass(frozen=True)
class ExportSettings:
    """Настройки экспорта в Excel.

    Attributes:
        export_path: Путь к выходному Excel-файлу.
    """

    export_path: str


@dataclass(frozen=True)
class LogSettings:
    """Настройки логирования.

    Attributes:
        level: Уровень логирования (DEBUG, INFO, WARNING, ERROR, CRITICAL).
        file_path: Путь к файлу логов (пустая строка — только консоль).
    """

    level: str
    file_path: str


@dataclass(frozen=True)
class ProxySettings:
    """Настройки ротации прокси и параллельной обработки.

    Attributes:
        proxy_file_path: Путь к файлу со списком прокси
            (формат: host:port:user:pass). Пустая строка — прокси отключены.
        rotate_every_n: Менять прокси каждые N обработанных карточек.
            0 — автоматическая ротация по счётчику отключена
            (ротация только при бане).
        max_workers: Максимальное количество параллельных браузерных
            сессий для обработки карточек объявлений. Каждый воркер
            использует свой прокси и имитирует отдельного пользователя.
            0 — автоопределение (равно количеству прокси, но не более 5).
            Если прокси не загружены — всегда 1 воркер.
    """

    proxy_file_path: str
    rotate_every_n: int
    max_workers: int


@dataclass(frozen=True)
class Settings:
    """Корневой объект конфигурации приложения.

    Объединяет все группы настроек в единую точку доступа.

    Attributes:
        browser: Настройки браузера.
        scraper: Настройки парсера.
        database: Настройки базы данных.
        export: Настройки экспорта.
        log: Настройки логирования.
        proxy: Настройки ротации прокси и параллельной обработки.
    """

    browser: BrowserSettings
    scraper: ScraperSettings
    database: DatabaseSettings
    export: ExportSettings
    log: LogSettings
    proxy: ProxySettings


def _parse_bool(value: str) -> bool:
    """Преобразует строковое значение в bool.

    Args:
        value: Строка для преобразования.

    Returns:
        True если значение 'true', '1', 'yes' (регистронезависимо).
    """
    return value.strip().lower() in ("true", "1", "yes")


def _parse_int(value: str, param_name: str) -> int:
    """Преобразует строковое значение в int с валидацией.

    Args:
        value: Строка для преобразования.
        param_name: Имя параметра для сообщения об ошибке.

    Returns:
        Целочисленное значение.

    Raises:
        ConfigValidationError: Если значение не является целым числом.
    """
    try:
        return int(value)
    except ValueError:
        raise ConfigValidationError(
            f"Параметр '{param_name}' должен быть целым числом, "
            f"получено: '{value}'"
        )


def _validate_required(value: str | None, param_name: str) -> str:
    """Проверяет, что обязательная переменная задана и не пуста.

    Args:
        value: Значение переменной окружения.
        param_name: Имя переменной для сообщения об ошибке.

    Returns:
        Непустое строковое значение.

    Raises:
        ConfigValidationError: Если переменная отсутствует или пуста.
    """
    if value is None or value.strip() == "":
        raise ConfigValidationError(
            f"Обязательная переменная окружения '{param_name}' не задана. "
            f"Проверьте файл .env (см. .env.example)."
        )
    return value.strip()


def _validate_log_level(value: str) -> str:
    """Проверяет корректность уровня логирования.

    Args:
        value: Строковое значение уровня.

    Returns:
        Валидный уровень логирования в верхнем регистре.

    Raises:
        ConfigValidationError: Если уровень не входит в допустимые.
    """
    valid_levels = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
    normalized = value.strip().upper()
    if normalized not in valid_levels:
        raise ConfigValidationError(
            f"Уровень логирования '{value}' недопустим. "
            f"Допустимые значения: {', '.join(valid_levels)}"
        )
    return normalized


def _validate_positive_int(value: int, param_name: str) -> int:
    """Проверяет, что число положительное.

    Args:
        value: Целочисленное значение.
        param_name: Имя параметра для сообщения об ошибке.

    Returns:
        Положительное целое число.

    Raises:
        ConfigValidationError: Если значение не положительное.
    """
    if value <= 0:
        raise ConfigValidationError(
            f"Параметр '{param_name}' должен быть положительным числом, "
            f"получено: {value}"
        )
    return value


def _validate_non_negative_int(value: int, param_name: str) -> int:
    """Проверяет, что число неотрицательное.

    Args:
        value: Целочисленное значение.
        param_name: Имя параметра для сообщения об ошибке.

    Returns:
        Неотрицательное целое число.

    Raises:
        ConfigValidationError: Если значение отрицательное.
    """
    if value < 0:
        raise ConfigValidationError(
            f"Параметр '{param_name}' не может быть отрицательным, "
            f"получено: {value}"
        )
    return value


def _validate_proxy_file(path: str) -> str:
    """Проверяет существование файла прокси, если путь указан.

    Args:
        path: Путь к файлу прокси (уже разрешённый в абсолютный).

    Returns:
        Валидированный путь к файлу.

    Raises:
        ConfigValidationError: Если файл не найден.
    """
    if not path:
        return path

    proxy_path = Path(path)
    if not proxy_path.exists():
        raise ConfigValidationError(
            f"Файл прокси не найден по пути '{path}'. "
            f"Создайте файл или оставьте PROXY_FILE_PATH пустым "
            f"для работы без прокси."
        )

    if not proxy_path.is_file():
        raise ConfigValidationError(
            f"Путь '{path}' не является файлом."
        )

    return path


def load_settings() -> Settings:
    """Загружает и валидирует все настройки приложения.

    Читает переменные окружения из .env файла, проверяет обязательные
    параметры, парсит типы и возвращает иммутабельный объект Settings.

    Все относительные пути (DB_PATH, EXPORT_PATH, LOG_FILE_PATH,
    PROXY_FILE_PATH) разрешаются относительно корня проекта,
    чтобы файлы создавались в правильном месте независимо
    от рабочей директории процесса.

    Returns:
        Полностью валидированный объект Settings.

    Raises:
        ConfigValidationError: Если обязательные переменные отсутствуют
            или значения параметров некорректны.
    """
    _load_env()

    errors: list[str] = []

    # --- Обязательные переменные ---
    try:
        category_url = _validate_required(
            os.getenv("AVITO_CATEGORY_URL"), "AVITO_CATEGORY_URL"
        )
    except ConfigValidationError as e:
        errors.append(str(e))
        category_url = ""

    # --- Браузер ---
    headless = _parse_bool(os.getenv("HEADLESS_MODE", "false"))
    nav_timeout_raw = os.getenv("NAVIGATION_TIMEOUT", "90000")
    page_wait_raw = os.getenv("PAGE_WAIT_TIME", "30000")

    try:
        nav_timeout = _validate_positive_int(
            _parse_int(nav_timeout_raw, "NAVIGATION_TIMEOUT"),
            "NAVIGATION_TIMEOUT",
        )
    except ConfigValidationError as e:
        errors.append(str(e))
        nav_timeout = 90000

    try:
        page_wait = _validate_positive_int(
            _parse_int(page_wait_raw, "PAGE_WAIT_TIME"),
            "PAGE_WAIT_TIME",
        )
    except ConfigValidationError as e:
        errors.append(str(e))
        page_wait = 30000

    # --- Скрапер ---
    max_pages_raw = os.getenv("MAX_PAGES", "0")
    try:
        max_pages = _validate_non_negative_int(
            _parse_int(max_pages_raw, "MAX_PAGES"),
            "MAX_PAGES",
        )
    except ConfigValidationError as e:
        errors.append(str(e))
        max_pages = 0

    # --- База данных (относительный путь → абсолютный от корня проекта) ---
    db_path = _resolve_path(os.getenv("DB_PATH", "data/avito_listings.db"))

    # --- Экспорт (относительный путь → абсолютный от корня проекта) ---
    export_path = _resolve_path(
        os.getenv("EXPORT_PATH", "data/avito_report.xlsx")
    )

    # --- Логирование (относительный путь → абсолютный от корня проекта) ---
    log_level_raw = os.getenv("LOG_LEVEL", "INFO")
    log_file_path = _resolve_path(
        os.getenv("LOG_FILE_PATH", "logs/app.log")
    )

    try:
        log_level = _validate_log_level(log_level_raw)
    except ConfigValidationError as e:
        errors.append(str(e))
        log_level = "INFO"

    # --- Прокси (относительный путь → абсолютный от корня проекта) ---
    proxy_file_path_raw = _resolve_path(
        os.getenv("PROXY_FILE_PATH", "")
    )
    rotate_every_n_raw = os.getenv("ROTATE_EVERY_N_LISTINGS", "70")
    max_workers_raw = os.getenv("MAX_WORKERS", "0")

    try:
        proxy_file_path = _validate_proxy_file(proxy_file_path_raw)
    except ConfigValidationError as e:
        errors.append(str(e))
        proxy_file_path = ""

    try:
        rotate_every_n = _validate_non_negative_int(
            _parse_int(rotate_every_n_raw, "ROTATE_EVERY_N_LISTINGS"),
            "ROTATE_EVERY_N_LISTINGS",
        )
    except ConfigValidationError as e:
        errors.append(str(e))
        rotate_every_n = 70

    try:
        max_workers = _validate_non_negative_int(
            _parse_int(max_workers_raw, "MAX_WORKERS"),
            "MAX_WORKERS",
        )
    except ConfigValidationError as e:
        errors.append(str(e))
        max_workers = 0

    # --- Если есть ошибки — выбрасываем все разом ---
    if errors:
        error_message = "Ошибки конфигурации:\n" + "\n".join(
            f"  - {err}" for err in errors
        )
        raise ConfigValidationError(error_message)

    return Settings(
        browser=BrowserSettings(
            headless=headless,
            navigation_timeout=nav_timeout,
            page_wait_time=page_wait,
        ),
        scraper=ScraperSettings(
            category_url=category_url,
            max_pages=max_pages,
        ),
        database=DatabaseSettings(
            db_path=db_path,
        ),
        export=ExportSettings(
            export_path=export_path,
        ),
        log=LogSettings(
            level=log_level,
            file_path=log_file_path,
        ),
        proxy=ProxySettings(
            proxy_file_path=proxy_file_path,
            rotate_every_n=rotate_every_n,
            max_workers=max_workers,
        ),
    )
