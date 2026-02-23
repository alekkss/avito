"""Сервис нормализации названий товаров через AI.

Координирует взаимодействие между репозиторием и AI-сервисом:
извлекает ненормализованные товары, разбивает на батчи,
отправляет на анализ AI и сохраняет результаты.

Паттерн Service Layer: инкапсулирует бизнес-логику нормализации
и оркестрирует работу нижележащих компонентов.
"""

import asyncio

from src.config import AISettings, get_logger
from src.models import NormalizedProduct, RawProduct, raw_to_normalized
from src.repositories.base import BaseProductRepository
from src.services.ai_service import AIProductResult, AIService, AIServiceError

logger = get_logger("normalizer_service")


class NormalizerService:
    """Сервис для батчевой нормализации товаров через AI.

    Оркестрирует полный цикл нормализации: чтение сырых данных,
    формирование батчей, отправка в AI, маппинг результатов
    и сохранение в репозиторий.

    Attributes:
        _ai_service: Клиент AI API для нормализации.
        _repository: Репозиторий для чтения и сохранения товаров.
        _batch_size: Размер батча для одного AI-запроса.
        _retry_delay: Задержка между батчами для снижения нагрузки.
    """

    def __init__(
        self,
        ai_service: AIService,
        repository: BaseProductRepository,
        ai_settings: AISettings,
    ) -> None:
        """Инициализирует сервис нормализации.

        Args:
            ai_service: Клиент AI API.
            repository: Репозиторий товаров.
            ai_settings: Настройки AI (batch_size, retry_delay).
        """
        self._ai_service = ai_service
        self._repository = repository
        self._batch_size = ai_settings.batch_size
        self._retry_delay = ai_settings.retry_delay

    async def normalize_all(self) -> list[NormalizedProduct]:
        """Нормализует все товары, ещё не прошедшие обработку.

        Основной публичный метод. Извлекает из репозитория товары
        без нормализации, разбивает на батчи и последовательно
        отправляет в AI.

        Returns:
            Список всех нормализованных товаров (новых).
        """
        raw_products = self._repository.get_raw_products_without_normalization()

        if not raw_products:
            logger.info("no_products_to_normalize")
            return []

        logger.info(
            "normalization_started",
            total_products=len(raw_products),
            batch_size=self._batch_size,
        )

        batches = self._split_into_batches(raw_products)
        all_normalized: list[NormalizedProduct] = []

        for batch_index, batch in enumerate(batches, start=1):
            logger.info(
                "processing_batch",
                batch_number=batch_index,
                total_batches=len(batches),
                batch_size=len(batch),
            )

            normalized = await self._process_batch(batch, batch_index)
            all_normalized.extend(normalized)

            # Задержка между батчами для снижения нагрузки на API
            if batch_index < len(batches):
                await asyncio.sleep(self._retry_delay)

        logger.info(
            "normalization_completed",
            total_raw=len(raw_products),
            total_normalized=len(all_normalized),
            success_rate=self._calculate_rate(
                len(all_normalized), len(raw_products)
            ),
        )

        return all_normalized

    def _split_into_batches(
        self, products: list[RawProduct]
    ) -> list[list[RawProduct]]:
        """Разбивает список товаров на батчи заданного размера.

        Args:
            products: Полный список товаров для нормализации.

        Returns:
            Список батчей, каждый содержит не более batch_size товаров.
        """
        batches: list[list[RawProduct]] = []
        for i in range(0, len(products), self._batch_size):
            batch = products[i:i + self._batch_size]
            batches.append(batch)

        logger.debug(
            "batches_created",
            total_products=len(products),
            total_batches=len(batches),
            batch_size=self._batch_size,
        )

        return batches

    async def _process_batch(
        self,
        batch: list[RawProduct],
        batch_index: int,
    ) -> list[NormalizedProduct]:
        """Обрабатывает один батч товаров через AI.

        Формирует данные для AI, отправляет запрос, маппит результаты
        на исходные товары и сохраняет в репозиторий.

        Args:
            batch: Список сырых товаров в батче.
            batch_index: Номер батча для логирования.

        Returns:
            Список успешно нормализованных товаров из этого батча.
        """
        products_for_ai = self._prepare_for_ai(batch)

        try:
            ai_results = await self._ai_service.normalize_batch(
                products_for_ai
            )
        except AIServiceError as e:
            logger.error(
                "batch_ai_request_failed",
                batch_number=batch_index,
                error=str(e),
            )
            return []

        normalized = self._map_results_to_products(batch, ai_results)

        if normalized:
            self._repository.save_normalized_products(normalized)
            logger.info(
                "batch_saved",
                batch_number=batch_index,
                saved_count=len(normalized),
            )

        return normalized

    def _prepare_for_ai(
        self, products: list[RawProduct]
    ) -> list[dict[str, str]]:
        """Подготавливает данные товаров для отправки в AI.

        Преобразует RawProduct в словари с полями, необходимыми
        для AI-промпта.

        Args:
            products: Список сырых товаров.

        Returns:
            Список словарей с avito_id, title, description.
        """
        return [
            {
                "avito_id": p.avito_id,
                "title": p.title,
                "description": p.description[:500],
            }
            for p in products
        ]

    def _map_results_to_products(
        self,
        raw_products: list[RawProduct],
        ai_results: list[AIProductResult],
    ) -> list[NormalizedProduct]:
        """Сопоставляет результаты AI с исходными товарами.

        Строит индекс сырых товаров по avito_id и для каждого
        результата AI создаёт NormalizedProduct, объединяя
        оригинальные данные с AI-нормализацией.

        Args:
            raw_products: Исходные сырые товары батча.
            ai_results: Результаты нормализации от AI.

        Returns:
            Список NormalizedProduct для успешно сопоставленных товаров.
        """
        raw_index: dict[str, RawProduct] = {
            p.avito_id: p for p in raw_products
        }

        normalized: list[NormalizedProduct] = []
        matched_ids: set[str] = set()

        for result in ai_results:
            raw = raw_index.get(result.avito_id)
            if raw is None:
                logger.warning(
                    "ai_result_no_matching_raw",
                    avito_id=result.avito_id,
                )
                continue

            product = raw_to_normalized(
                raw=raw,
                normalized_title=result.normalized_title,
                product_category=result.product_category,
                key_specs=result.key_specs,
            )
            normalized.append(product)
            matched_ids.add(result.avito_id)

        unmatched = set(raw_index.keys()) - matched_ids
        if unmatched:
            logger.warning(
                "products_not_normalized",
                unmatched_count=len(unmatched),
                unmatched_ids=list(unmatched)[:10],
            )

        return normalized

    def _calculate_rate(self, part: int, total: int) -> str:
        """Вычисляет процент успешности.

        Args:
            part: Количество успешных элементов.
            total: Общее количество элементов.

        Returns:
            Строка с процентом, например "85.0%".
        """
        if total == 0:
            return "0.0%"
        rate = (part / total) * 100
        return f"{rate:.1f}%"
