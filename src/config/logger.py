"""Модуль логирования с человекочитаемым форматом.

Консольный вывод — понятные цветные сообщения на русском.
Файловый вывод (опционально) — JSON для машинного анализа.

Пример вывода в консоль:
    [09:27:36] ✅ INFO     Страница спарсена | page=5 items=50 total=250
    [09:27:38] ⚠️  WARNING  Кнопка пагинации не найдена, перезагрузка | attempt=3
    [09:27:40] ❌ ERROR    Ошибка AI API | status=500 retry=2/3
"""

import json
import logging
import sys
import uuid
from contextvars import ContextVar
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


_trace_id_var: ContextVar[str] = ContextVar("trace_id", default="")


def set_trace_id(trace_id: str | None = None) -> str:
    """Устанавливает trace_id для текущего контекста выполнения.

    Args:
        trace_id: Идентификатор трассировки. Если None — генерируется UUID4.

    Returns:
        Установленный trace_id.
    """
    if trace_id is None:
        trace_id = uuid.uuid4().hex[:8]
    _trace_id_var.set(trace_id)
    return trace_id


def get_trace_id() -> str:
    """Возвращает trace_id текущего контекста."""
    return _trace_id_var.get()


# Цвета ANSI для терминала
class _Colors:
    """ANSI escape-коды для цветного вывода в терминал."""

    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"

    # Цвета текста
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    MAGENTA = "\033[35m"
    CYAN = "\033[36m"
    WHITE = "\033[37m"
    GRAY = "\033[90m"

    # Яркие цвета
    BRIGHT_RED = "\033[91m"
    BRIGHT_GREEN = "\033[92m"
    BRIGHT_YELLOW = "\033[93m"
    BRIGHT_CYAN = "\033[96m"


# Маппинг уровней на иконки и цвета
_LEVEL_STYLES: dict[str, tuple[str, str]] = {
    "DEBUG":    ("🔍", _Colors.GRAY),
    "INFO":     ("✅", _Colors.BRIGHT_GREEN),
    "WARNING":  ("⚠️ ", _Colors.BRIGHT_YELLOW),
    "ERROR":    ("❌", _Colors.BRIGHT_RED),
    "CRITICAL": ("🔥", _Colors.BOLD + _Colors.BRIGHT_RED),
}


class HumanFormatter(logging.Formatter):
    """Форматирует логи в человекочитаемый цветной формат для консоли.

    Формат:
        [ЧЧ:ММ:СС] 🔍 LEVEL    Сообщение | ключ=значение ключ=значение
    """

    def format(self, record: logging.LogRecord) -> str:
        """Форматирует LogRecord в читаемую строку.

        Args:
            record: Запись лога.

        Returns:
            Цветная человекочитаемая строка.
        """
        now = datetime.now(timezone.utc).strftime("%H:%M:%S")
        icon, color = _LEVEL_STYLES.get(
            record.levelname, ("  ", _Colors.WHITE)
        )
        level = record.levelname.ljust(8)

        # Основное сообщение
        message = record.getMessage()

        # Контекстные данные из kwargs
        context_parts: list[str] = []
        extra_data: dict[str, Any] = getattr(record, "context_data", {})
        if extra_data:
            for key, value in extra_data.items():
                context_parts.append(
                    f"{_Colors.CYAN}{key}{_Colors.RESET}="
                    f"{_Colors.WHITE}{value}{_Colors.RESET}"
                )

        # Собираем строку
        context_str = ""
        if context_parts:
            context_str = f" {_Colors.DIM}|{_Colors.RESET} " + "  ".join(
                context_parts
            )

        # Информация об исключении
        exc_str = ""
        if record.exc_info and record.exc_info[1] is not None:
            exc_type = type(record.exc_info[1]).__name__
            exc_msg = str(record.exc_info[1])
            exc_str = (
                f"\n   {_Colors.RED}└─ {exc_type}: "
                f"{exc_msg}{_Colors.RESET}"
            )

        return (
            f"{_Colors.DIM}[{now}]{_Colors.RESET} "
            f"{icon} {color}{level}{_Colors.RESET} "
            f"{message}{context_str}{exc_str}"
        )


class JSONFileFormatter(logging.Formatter):
    """Форматирует логи в JSON для файлового вывода."""

    def format(self, record: logging.LogRecord) -> str:
        """Форматирует LogRecord в JSON-строку для файла.

        Args:
            record: Запись лога.

        Returns:
            JSON-строка.
        """
        log_entry: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "trace_id": get_trace_id(),
            "logger": record.name,
        }

        extra_data: dict[str, Any] = getattr(record, "context_data", {})
        if extra_data:
            log_entry["context"] = extra_data

        if record.exc_info and record.exc_info[1] is not None:
            log_entry["exception"] = {
                "type": type(record.exc_info[1]).__name__,
                "message": str(record.exc_info[1]),
            }

        return json.dumps(log_entry, ensure_ascii=False, default=str)


class ContextLogger:
    """Обёртка над стандартным логгером с поддержкой контекстных полей.

    Позволяет передавать kwargs, которые отображаются
    как ключ=значение в конце строки лога.

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
        """Лог уровня DEBUG."""
        self._log(logging.DEBUG, message, **kwargs)

    def info(self, message: str, **kwargs: Any) -> None:
        """Лог уровня INFO."""
        self._log(logging.INFO, message, **kwargs)

    def warning(self, message: str, **kwargs: Any) -> None:
        """Лог уровня WARNING."""
        self._log(logging.WARNING, message, **kwargs)

    def error(
        self, message: str, exc_info: bool = False, **kwargs: Any
    ) -> None:
        """Лог уровня ERROR."""
        self._log(logging.ERROR, message, exc_info=exc_info, **kwargs)

    def critical(
        self, message: str, exc_info: bool = False, **kwargs: Any
    ) -> None:
        """Лог уровня CRITICAL."""
        self._log(logging.CRITICAL, message, exc_info=exc_info, **kwargs)


_loggers: dict[str, ContextLogger] = {}


def setup_logging(level: str = "INFO", log_file_path: str = "") -> None:
    """Настраивает логирование.

    Консоль — человекочитаемый цветной формат.
    Файл (опционально) — JSON для машинного анализа.

    Args:
        level: Уровень логирования.
        log_file_path: Путь к файлу логов. Пустая строка — только консоль.
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    root_logger.handlers.clear()

    # Консоль — человекочитаемый формат
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(HumanFormatter())
    root_logger.addHandler(console_handler)

    # Файл — JSON формат (если задан путь)
    if log_file_path:
        log_path = Path(log_file_path)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(
            str(log_path), encoding="utf-8"
        )
        file_handler.setFormatter(JSONFileFormatter())
        root_logger.addHandler(file_handler)


def get_logger(name: str) -> ContextLogger:
    """Возвращает именованный логгер.

    Args:
        name: Имя логгера (например, 'scraper', 'ai_service').

    Returns:
        Экземпляр ContextLogger.
    """
    if name not in _loggers:
        stdlib_logger = logging.getLogger(name)
        _loggers[name] = ContextLogger(stdlib_logger)
    return _loggers[name]
