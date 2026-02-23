"""Модуль конфигурации приложения.

Загружает переменные окружения из .env файла, валидирует обязательные
параметры и предоставляет единый объект Settings для доступа ко всем
настройкам приложения.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv


def _load_env() -> None:
    """Загружает переменные окружения из .env файла.

    Ищет .env файл в корне проекта (два уровня вверх от этого модуля).
    Если файл не найден, переменные берутся из системного окружения.
    """
    project_root = Path(__file__).resolve().parent.parent.parent
    env_path = project_root / ".env"
    load_dotenv(dotenv_path=env_path)


class ConfigValidationError(Exception):
    """Ошибка валидации конфигурации.

    Выбрасывается при отсутствии обязательных переменных окружения
    или при некорректных значениях параметров.
    """


@dataclass(frozen=True)
class AISettings:
    """Настройки подключения к AI API (OpenRouter).

    Attributes:
        api_url: URL эндпоинта chat completions.
        api_key: Секретный ключ API.
        model: Идентификатор модели AI.
        max_retries: Максимальное число повторных попыток при ошибке.
        retry_delay: Задержка между попытками в секундах.
        batch_size: Количество товаров в одном запросе к AI.
    """

    api_url: str
    api_key: str
    model: str
    max_retries: int
    retry_delay: float
    batch_size: int


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
class Settings:
    """Корневой объект конфигурации приложения.

    Объединяет все группы настроек в единую точку доступа.

    Attributes:
        ai: Настройки AI API.
        browser: Настройки браузера.
        scraper: Настройки парсера.
        database: Настройки базы данных.
        export: Настройки экспорта.
        log: Настройки логирования.
    """

    ai: AISettings
    browser: BrowserSettings
    scraper: ScraperSettings
    database: DatabaseSettings
    export: ExportSettings
    log: LogSettings


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


def _parse_float(value: str, param_name: str) -> float:
    """Преобразует строковое значение в float с валидацией.

    Args:
        value: Строка для преобразования.
        param_name: Имя параметра для сообщения об ошибке.

    Returns:
        Числовое значение с плавающей точкой.

    Raises:
        ConfigValidationError: Если значение не является числом.
    """
    try:
        return float(value)
    except ValueError:
        raise ConfigValidationError(
            f"Параметр '{param_name}' должен быть числом, "
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


def load_settings() -> Settings:
    """Загружает и валидирует все настройки приложения.

    Читает переменные окружения из .env файла, проверяет обязательные
    параметры, парсит типы и возвращает иммутабельный объект Settings.

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
        ai_api_key = _validate_required(
            os.getenv("AI_API_KEY"), "AI_API_KEY"
        )
    except ConfigValidationError as e:
        errors.append(str(e))
        ai_api_key = ""

    try:
        category_url = _validate_required(
            os.getenv("AVITO_CATEGORY_URL"), "AVITO_CATEGORY_URL"
        )
    except ConfigValidationError as e:
        errors.append(str(e))
        category_url = ""

    # --- AI (необязательные с дефолтами) ---
    ai_api_url = os.getenv(
        "AI_API_URL", "https://openrouter.ai/api/v1/chat/completions"
    )
    ai_model = os.getenv("AI_MODEL", "qwen/qwen3.5-plus-02-15")

    ai_max_retries_raw = os.getenv("AI_MAX_RETRIES", "3")
    ai_retry_delay_raw = os.getenv("AI_RETRY_DELAY", "2.0")
    ai_batch_size_raw = os.getenv("AI_BATCH_SIZE", "10")

    try:
        ai_max_retries = _validate_positive_int(
            _parse_int(ai_max_retries_raw, "AI_MAX_RETRIES"),
            "AI_MAX_RETRIES",
        )
    except ConfigValidationError as e:
        errors.append(str(e))
        ai_max_retries = 3

    try:
        ai_retry_delay = _parse_float(ai_retry_delay_raw, "AI_RETRY_DELAY")
        if ai_retry_delay < 0:
            raise ConfigValidationError(
                "Параметр 'AI_RETRY_DELAY' не может быть отрицательным"
            )
    except ConfigValidationError as e:
        errors.append(str(e))
        ai_retry_delay = 2.0

    try:
        ai_batch_size = _validate_positive_int(
            _parse_int(ai_batch_size_raw, "AI_BATCH_SIZE"),
            "AI_BATCH_SIZE",
        )
    except ConfigValidationError as e:
        errors.append(str(e))
        ai_batch_size = 10

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

    # --- База данных ---
    db_path = os.getenv("DB_PATH", "data/avito_products.db")

    # --- Экспорт ---
    export_path = os.getenv("EXPORT_PATH", "data/avito_report.xlsx")

    # --- Логирование ---
    log_level_raw = os.getenv("LOG_LEVEL", "INFO")
    log_file_path = os.getenv("LOG_FILE_PATH", "")

    try:
        log_level = _validate_log_level(log_level_raw)
    except ConfigValidationError as e:
        errors.append(str(e))
        log_level = "INFO"

    # --- Если есть ошибки — выбрасываем все разом ---
    if errors:
        error_message = "Ошибки конфигурации:\n" + "\n".join(
            f"  - {err}" for err in errors
        )
        raise ConfigValidationError(error_message)

    return Settings(
        ai=AISettings(
            api_url=ai_api_url,
            api_key=ai_api_key,
            model=ai_model,
            max_retries=ai_max_retries,
            retry_delay=ai_retry_delay,
            batch_size=ai_batch_size,
        ),
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
    )
