"""Клиент для работы с OpenRouter AI API.

Отправляет запросы к AI-модели для анализа и нормализации
названий товаров. Поддерживает батчевую обработку нескольких
товаров за один запрос и retry при сбоях.

Паттерн Strategy: можно заменить реализацию AI-клиента
(например, на прямой вызов OpenAI или локальную модель)
без изменения остального кода.
"""

import json
from dataclasses import dataclass

import aiohttp

from src.config import AISettings, get_logger
from src.utils import async_retry

logger = get_logger("ai_service")


@dataclass(frozen=True)
class AIProductResult:
    """Результат AI-нормализации одного товара.

    Attributes:
        avito_id: Идентификатор товара на Avito.
        normalized_title: Нормализованное название для группировки.
        product_category: Категория товара.
        key_specs: Ключевые характеристики товара.
    """

    avito_id: str
    normalized_title: str
    product_category: str
    key_specs: str


class AIServiceError(Exception):
    """Ошибка при взаимодействии с AI API.

    Выбрасывается при сетевых ошибках, невалидных ответах
    или превышении лимитов API.
    """


NORMALIZATION_PROMPT: str = """Ты — эксперт по классификации товаров. Твоя задача — проанализировать список товаров с Avito и для каждого определить:

1. **normalized_title** — унифицированное название товара (модель), по которому можно группировать одинаковые товары. Формат: "Бренд Модель Ключевая_характеристика". Например: "MSI Thin A15 RTX 4060", "Apple MacBook Air M2 256GB", "Lenovo ThinkPad T14 i5-1335U".

2. **product_category** — категория товара. Например: "Ноутбук", "Игровой ноутбук", "Ультрабук".

3. **key_specs** — ключевые характеристики через запятую, извлечённые из названия и описания. Например: "RTX 4060, 16GB RAM, 512GB SSD, 15.6 дюймов".

ВАЖНЫЕ ПРАВИЛА:
- Если два товара — это одна и та же модель устройства (одинаковый бренд, модельный ряд и ключевые характеристики), у них ДОЛЖЕН быть ОДИНАКОВЫЙ normalized_title.
- Игнорируй слова типа "срочно", "б/у", "новый", "идеальное состояние" — они не влияют на модель.
- Если из названия/описания невозможно точно определить модель, используй максимально общее название.
- Извлекай характеристики из описания, если в названии их нет.

Ответь СТРОГО в формате JSON-массива. Никакого текста до или после JSON.

Формат ответа:
[
  {
    "avito_id": "123456",
    "normalized_title": "Бренд Модель Характеристика",
    "product_category": "Категория",
    "key_specs": "характеристика1, характеристика2"
  }
]

Вот список товаров для анализа:
"""


class AIService:
    """Сервис для нормализации товаров через OpenRouter AI API.

    Отправляет батчи товаров на анализ AI-модели и парсит
    структурированные ответы. Использует retry при сбоях
    и валидирует формат ответа.

    Attributes:
        _settings: Настройки AI API (URL, ключ, модель).
        _session: Общая aiohttp-сессия для переиспользования соединений.
    """

    def __init__(self, settings: AISettings) -> None:
        """Инициализирует AI-сервис.

        Args:
            settings: Настройки подключения к AI API.
        """
        self._settings = settings
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        """Возвращает активную aiohttp-сессию.

        Создаёт сессию при первом вызове и переиспользует
        для последующих запросов.

        Returns:
            Активная aiohttp-сессия.
        """
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=120),
            )
        return self._session

    def _build_products_text(
        self, products: list[dict[str, str]]
    ) -> str:
        """Формирует текстовое описание товаров для промпта.

        Args:
            products: Список словарей с полями avito_id, title, description.

        Returns:
            Форматированный текст со списком товаров.
        """
        lines: list[str] = []
        for item in products:
            lines.append(
                f"- ID: {item['avito_id']}\n"
                f"  Название: {item['title']}\n"
                f"  Описание: {item['description']}\n"
            )
        return "\n".join(lines)

    def _build_request_payload(self, user_message: str) -> dict:
        """Формирует тело запроса к OpenRouter API.

        Args:
            user_message: Полный текст сообщения пользователя с промптом.

        Returns:
            Словарь с параметрами запроса.
        """
        return {
            "model": self._settings.model,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "Ты — AI-ассистент для классификации товаров. "
                        "Отвечай только валидным JSON."
                    ),
                },
                {
                    "role": "user",
                    "content": user_message,
                },
            ],
            "temperature": 0.1,
            "max_tokens": 4096,
        }

    def _build_headers(self) -> dict[str, str]:
        """Формирует HTTP-заголовки для запроса к API.

        Returns:
            Словарь заголовков с авторизацией.
        """
        return {
            "Authorization": f"Bearer {self._settings.api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://github.com/avito-parser",
            "X-Title": "Avito Parser",
        }

    def _extract_json_from_response(self, text: str) -> list[dict]:
        """Извлекает JSON-массив из текстового ответа AI.

        AI может вернуть JSON обёрнутый в markdown-блок (```json ... ```)
        или с лишним текстом. Метод пытается найти и распарсить
        JSON-массив из ответа.

        Args:
            text: Текстовый ответ от AI-модели.

        Returns:
            Список словарей с результатами.

        Raises:
            AIServiceError: Если не удалось извлечь валидный JSON.
        """
        cleaned = text.strip()

        # Убираем markdown-обёртку ```json ... ```
        if "```json" in cleaned:
            start = cleaned.index("```json") + len("```json")
            end = cleaned.index("```", start)
            cleaned = cleaned[start:end].strip()
        elif "```" in cleaned:
            start = cleaned.index("```") + len("```")
            end = cleaned.index("```", start)
            cleaned = cleaned[start:end].strip()

        # Ищем JSON-массив в тексте
        bracket_start = cleaned.find("[")
        bracket_end = cleaned.rfind("]")

        if bracket_start == -1 or bracket_end == -1:
            raise AIServiceError(
                f"AI ответ не содержит JSON-массива: {text[:200]}"
            )

        json_str = cleaned[bracket_start:bracket_end + 1]

        try:
            result = json.loads(json_str)
        except json.JSONDecodeError as e:
            raise AIServiceError(
                f"Невалидный JSON в ответе AI: {e}. "
                f"Фрагмент: {json_str[:200]}"
            ) from e

        if not isinstance(result, list):
            raise AIServiceError(
                f"AI вернул не массив, а {type(result).__name__}"
            )

        return result

    def _parse_ai_results(
        self, raw_results: list[dict]
    ) -> list[AIProductResult]:
        """Преобразует сырые словари из AI-ответа в AIProductResult.

        Валидирует наличие обязательных полей в каждом элементе
        и создаёт типизированные объекты результатов.

        Args:
            raw_results: Список словарей из JSON-ответа AI.

        Returns:
            Список валидированных AIProductResult.
        """
        results: list[AIProductResult] = []
        required_fields = (
            "avito_id",
            "normalized_title",
            "product_category",
            "key_specs",
        )

        for item in raw_results:
            if not isinstance(item, dict):
                logger.warning(
                    "ai_result_not_dict",
                    item_type=type(item).__name__,
                )
                continue

            missing = [f for f in required_fields if f not in item]
            if missing:
                logger.warning(
                    "ai_result_missing_fields",
                    missing_fields=missing,
                    avito_id=item.get("avito_id", "unknown"),
                )
                continue

            results.append(
                AIProductResult(
                    avito_id=str(item["avito_id"]),
                    normalized_title=str(item["normalized_title"]).strip(),
                    product_category=str(item["product_category"]).strip(),
                    key_specs=str(item["key_specs"]).strip(),
                )
            )

        return results

    async def normalize_batch(
        self, products: list[dict[str, str]]
    ) -> list[AIProductResult]:
        """Нормализует батч товаров через AI API.

        Отправляет список товаров на анализ AI-модели и возвращает
        структурированные результаты нормализации.

        Args:
            products: Список словарей с ключами:
                - avito_id: ID товара
                - title: Название товара
                - description: Описание товара

        Returns:
            Список AIProductResult с нормализованными данными.

        Raises:
            AIServiceError: При критических ошибках API.
        """
        if not products:
            return []

        products_text = self._build_products_text(products)
        user_message = NORMALIZATION_PROMPT + products_text

        logger.info(
            "ai_batch_request_started",
            batch_size=len(products),
        )

        response_text = await self._send_request(user_message)
        raw_results = self._extract_json_from_response(response_text)
        results = self._parse_ai_results(raw_results)

        logger.info(
            "ai_batch_request_completed",
            batch_size=len(products),
            results_count=len(results),
        )

        return results

    async def _send_request(self, user_message: str) -> str:
        """Отправляет запрос к OpenRouter API с retry-логикой.

        Args:
            user_message: Полный текст запроса с промптом и данными.

        Returns:
            Текстовый ответ AI-модели.

        Raises:
            AIServiceError: Если все попытки исчерпаны или ответ невалиден.
        """

        @async_retry(
            max_retries=self._settings.max_retries,
            delay=self._settings.retry_delay,
            backoff_factor=2.0,
            exceptions=(aiohttp.ClientError, AIServiceError),
        )
        async def _do_request() -> str:
            session = await self._get_session()
            payload = self._build_request_payload(user_message)
            headers = self._build_headers()

            async with session.post(
                self._settings.api_url,
                json=payload,
                headers=headers,
            ) as response:
                if response.status != 200:
                    body = await response.text()
                    raise AIServiceError(
                        f"AI API вернул статус {response.status}: "
                        f"{body[:300]}"
                    )

                data = await response.json()

                choices = data.get("choices", [])
                if not choices:
                    raise AIServiceError(
                        "AI API вернул пустой choices: "
                        f"{json.dumps(data, ensure_ascii=False)[:300]}"
                    )

                content = (
                    choices[0]
                    .get("message", {})
                    .get("content", "")
                )
                if not content:
                    raise AIServiceError(
                        "AI API вернул пустой content в ответе"
                    )

                logger.debug(
                    "ai_raw_response",
                    response_length=len(content),
                    response_preview=content[:100],
                )

                return content

        return await _do_request()

    async def close(self) -> None:
        """Закрывает aiohttp-сессию и освобождает ресурсы."""
        if self._session is not None and not self._session.closed:
            await self._session.close()
            self._session = None
            logger.info("ai_session_closed")
