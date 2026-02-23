"""Модуль структурированного JSON-логирования.

Предоставляет фабрику логгеров с JSON-форматированием,
поддержкой trace_id для отслеживания цепочек операций
и контекстных полей для каждого сообщения.

Пример использования:
    logger = get_logger("scraper_service")
    logger.info("page_loaded", page=1, items_count=50)
"""

import json
import logging
import sys
import uuid
from contextvars import ContextVar
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


# Контекстная переменная для хранения trace_id текущей операции.
# Позволяет связывать логи одного запуска/операции без передачи
# trace_id через все функции вручную.
_trace_id_var: ContextVar[str] = ContextVar("trace_id", default="")


def set_trace_id(trace_id: str | None = None) -> str:
    """Устанавливает trace_id для текущего контекста выполнения.

    Args:
        trace_id: Идентификатор трассировки. Если None — генерируется
            автоматически (UUID4, первые 8 символов).

    Returns:
        Установленный trace_id.
    """
    if trace_id is None:
        trace_id = uuid.uuid4().hex[:8]
    _trace_id_var.set(trace_id)
    return trace_id


def get_trace_id() -> str:
    """Возвращает trace_id текущего контекста.

    Returns:
        Текущий trace_id или пустую строку, если не установлен.
    """
    return _trace_id_var.get()


class JSONFormatter(logging.Formatter):
    """Форматирует лог-записи в JSON-формат.

    Каждая запись содержит поля:
        - timestamp: время в ISO 8601 (UTC)
        - level: уровень логирования
        - message: текст сообщения
        - trace_id: идентификатор трассировки операции
        - logger: имя логгера (модуля)
        - context: дополнительные поля, переданные через extra
    """

    def format(self, record: logging.LogRecord) -> str:
        """Форматирует LogRecord в JSON-строку.

        Args:
            record: Стандартная запись лога Python.

        Returns:
            JSON-строка с полями лога.
        """
        log_entry: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "trace_id": get_trace_id(),
            "logger": record.name,
        }

        # Извлекаем контекстные поля из extra
        context: dict[str, Any] = {}
        extra_data: dict[str, Any] = getattr(record, "context_data", {})
        if extra_data:
            context.update(extra_data)

        # Добавляем информацию об исключении, если есть
        if record.exc_info and record.exc_info[1] is not None:
            context["exception_type"] = type(record.exc_info[1]).__name__
            context["exception_message"] = str(record.exc_info[1])

        if context:
            log_entry["context"] = context

        return json.dumps(log_entry, ensure_ascii=False, default=str)


class ContextLogger:
    """Обёртка над стандартным логгером с поддержкой контекстных полей.

    Позволяет передавать произвольные ключевые аргументы в методы
    логирования, которые автоматически попадают в поле context
    JSON-вывода.

    Attributes:
        _logger: Внутренний экземпляр стандартного логгера.
    """

    def __init__(self, logger: logging.Logger) -> None:
        """Инициализирует обёртку.

        Args:
            logger: Стандартный логгер Python.
        """
        self._logger = logger

    def _log(
        self,
        level: int,
        message: str,
        exc_info: bool = False,
        **kwargs: Any,
    ) -> None:
        """Формирует и отправляет лог-запись с контекстными данными.

        Args:
            level: Числовой уровень логирования.
            message: Текст сообщения.
            exc_info: Включать ли информацию об исключении.
            **kwargs: Дополнительные контекстные поля.
        """
        extra: dict[str, Any] = {"context_data": kwargs}
        self._logger.log(
            level, message, exc_info=exc_info, extra=extra
        )

    def debug(self, message: str, **kwargs: Any) -> None:
        """Лог уровня DEBUG.

        Args:
            message: Текст сообщения.
            **kwargs: Контекстные поля.
        """
        self._log(logging.DEBUG, message, **kwargs)

    def info(self, message: str, **kwargs: Any) -> None:
        """Лог уровня INFO.

        Args:
            message: Текст сообщения.
            **kwargs: Контекстные поля.
        """
        self._log(logging.INFO, message, **kwargs)

    def warning(self, message: str, **kwargs: Any) -> None:
        """Лог уровня WARNING.

        Args:
            message: Текст сообщения.
            **kwargs: Контекстные поля.
        """
        self._log(logging.WARNING, message, **kwargs)

    def error(self, message: str, exc_info: bool = False, **kwargs: Any) -> None:
        """Лог уровня ERROR.

        Args:
            message: Текст сообщения.
            exc_info: Включать ли стек-трейс исключения.
            **kwargs: Контекстные поля.
        """
        self._log(logging.ERROR, message, exc_info=exc_info, **kwargs)

    def critical(self, message: str, exc_info: bool = False, **kwargs: Any) -> None:
        """Лог уровня CRITICAL.

        Args:
            message: Текст сообщения.
            exc_info: Включать ли стек-трейс исключения.
            **kwargs: Контекстные поля.
        """
        self._log(logging.CRITICAL, message, exc_info=exc_info, **kwargs)


# Реестр уже созданных логгеров — предотвращает дублирование хендлеров.
_loggers: dict[str, ContextLogger] = {}


def setup_logging(level: str = "INFO", log_file_path: str = "") -> None:
    """Настраивает корневую конфигурацию логирования.

    Вызывается один раз при старте приложения. Устанавливает
    JSON-форматтер на корневой логгер, настраивает вывод в консоль
    и опционально в файл.

    Args:
        level: Уровень логирования (DEBUG, INFO, WARNING, ERROR, CRITICAL).
        log_file_path: Путь к файлу логов. Пустая строка — только консоль.
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Очищаем существующие хендлеры, чтобы избежать дублей при повторном вызове
    root_logger.handlers.clear()

    json_formatter = JSONFormatter()

    # Консольный вывод
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(json_formatter)
    root_logger.addHandler(console_handler)

    # Файловый вывод (если путь задан)
    if log_file_path:
        log_path = Path(log_file_path)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(
            str(log_path), encoding="utf-8"
        )
        file_handler.setFormatter(json_formatter)
        root_logger.addHandler(file_handler)


def get_logger(name: str) -> ContextLogger:
    """Фабрика логгеров — возвращает именованный логгер с JSON-форматом.

    Каждый модуль приложения получает свой логгер по имени.
    Повторный вызов с тем же именем возвращает тот же экземпляр.

    Args:
        name: Имя логгера (например, 'scraper_service', 'ai_service').

    Returns:
        Экземпляр ContextLogger с поддержкой контекстных полей.

    Пример:
        logger = get_logger("scraper_service")
        logger.info("page_loaded", page=1, items_count=50)
        # Вывод: {"timestamp": "...", "level": "INFO",
        #   "message": "page_loaded", "trace_id": "a1b2c3d4",
        #   "logger": "scraper_service",
        #   "context": {"page": 1, "items_count": 50}}
    """
    if name not in _loggers:
        stdlib_logger = logging.getLogger(name)
        _loggers[name] = ContextLogger(stdlib_logger)
    return _loggers[name]
