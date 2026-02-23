"""Пакет сервисов бизнес-логики.

Предоставляет все сервисы приложения:
    from src.services import (
        BrowserService,
        ScraperService,
        AIService,
        NormalizerService,
        ExportService,
    )
"""

from src.services.ai_service import AIProductResult, AIService, AIServiceError
from src.services.browser_service import BrowserService
from src.services.export_service import ExportService
from src.services.normalizer_service import NormalizerService
from src.services.scraper_service import ScraperService

__all__ = [
    "AIProductResult",
    "AIService",
    "AIServiceError",
    "BrowserService",
    "ExportService",
    "NormalizerService",
    "ScraperService",
]
