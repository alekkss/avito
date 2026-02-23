"""Пакет конфигурации приложения.

Предоставляет централизованный доступ к настройкам и логированию:
    from src.config import load_settings, get_logger, setup_logging
"""

from src.config.logger import get_logger, set_trace_id, setup_logging
from src.config.settings import (
    AISettings,
    BrowserSettings,
    ConfigValidationError,
    DatabaseSettings,
    ExportSettings,
    LogSettings,
    ScraperSettings,
    Settings,
    load_settings,
)

__all__ = [
    "AISettings",
    "BrowserSettings",
    "ConfigValidationError",
    "DatabaseSettings",
    "ExportSettings",
    "LogSettings",
    "ScraperSettings",
    "Settings",
    "get_logger",
    "load_settings",
    "set_trace_id",
    "setup_logging",
]
