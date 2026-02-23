"""SQLite-реализация репозитория товаров.

Хранит сырые и нормализованные товары в двух таблицах SQLite.
Поддерживает upsert-логику (вставка или обновление по avito_id),
батчевое сохранение и выборки для AI-нормализации.
"""

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from src.config import get_logger
from src.models import NormalizedProduct, RawProduct
from src.repositories.base import BaseProductRepository

logger = get_logger("sqlite_repository")


class SQLiteProductRepository(BaseProductRepository):
    """Репозиторий товаров на базе SQLite.

    Создаёт файл базы данных по указанному пути, автоматически
    создаёт необходимые таблицы при инициализации.

    Attributes:
        _db_path: Путь к файлу базы данных.
        _connection: Активное соединение с SQLite.
    """

    def __init__(self, db_path: str) -> None:
        """Инициализирует репозиторий.

        Args:
            db_path: Путь к файлу SQLite базы данных.
        """
        self._db_path = db_path
        self._connection: sqlite3.Connection | None = None

    def _get_connection(self) -> sqlite3.Connection:
        """Возвращает активное соединение с базой данных.

        Если соединение ещё не создано — создаёт его.

        Returns:
            Активное соединение SQLite.

        Raises:
            RuntimeError: Если не удалось установить соединение.
        """
        if self._connection is None:
            try:
                db_dir = Path(self._db_path).parent
                db_dir.mkdir(parents=True, exist_ok=True)

                self._connection = sqlite3.connect(self._db_path)
                self._connection.row_factory = sqlite3.Row
                self._connection.execute("PRAGMA journal_mode=WAL")
                self._connection.execute("PRAGMA foreign_keys=ON")

                logger.info(
                    "database_connected",
                    db_path=self._db_path,
                )
            except sqlite3.Error as e:
                logger.error(
                    "database_connection_failed",
                    exc_info=True,
                    db_path=self._db_path,
                    error=str(e),
                )
                raise RuntimeError(
                    f"Не удалось подключиться к БД: {self._db_path}"
                ) from e
        return self._connection

    def initialize(self) -> None:
        """Создаёт таблицы и индексы, если они ещё не существуют."""
        conn = self._get_connection()

        create_raw_table = """
        CREATE TABLE IF NOT EXISTS raw_products (
            avito_id      TEXT PRIMARY KEY,
            title         TEXT NOT NULL,
            price         INTEGER NOT NULL,
            url           TEXT NOT NULL,
            description   TEXT DEFAULT '',
            image_url     TEXT DEFAULT '',
            seller_name   TEXT DEFAULT '',
            seller_rating TEXT DEFAULT '',
            seller_reviews TEXT DEFAULT '',
            scraped_at    TEXT NOT NULL
        )
        """

        create_normalized_table = """
        CREATE TABLE IF NOT EXISTS normalized_products (
            avito_id          TEXT PRIMARY KEY,
            original_title    TEXT NOT NULL,
            normalized_title  TEXT NOT NULL,
            product_category  TEXT NOT NULL,
            key_specs         TEXT NOT NULL,
            price             INTEGER NOT NULL,
            url               TEXT NOT NULL,
            full_url          TEXT NOT NULL,
            description       TEXT DEFAULT '',
            image_url         TEXT DEFAULT '',
            seller_name       TEXT DEFAULT '',
            seller_rating     TEXT DEFAULT '',
            seller_reviews    TEXT DEFAULT '',
            scraped_at        TEXT NOT NULL,
            normalized_at     TEXT NOT NULL
        )
        """

        create_index_normalized_title = """
        CREATE INDEX IF NOT EXISTS idx_normalized_title
        ON normalized_products (normalized_title)
        """

        create_index_category = """
        CREATE INDEX IF NOT EXISTS idx_product_category
        ON normalized_products (product_category)
        """

        try:
            conn.execute(create_raw_table)
            conn.execute(create_normalized_table)
            conn.execute(create_index_normalized_title)
            conn.execute(create_index_category)
            conn.commit()

            logger.info("database_initialized", db_path=self._db_path)
        except sqlite3.Error as e:
            logger.error(
                "database_init_failed",
                exc_info=True,
                error=str(e),
            )
            raise RuntimeError(
                "Не удалось инициализировать таблицы БД"
            ) from e

    def _serialize_datetime(self, dt: datetime) -> str:
        """Сериализует datetime в ISO-строку для хранения в SQLite.

        Args:
            dt: Объект datetime.

        Returns:
            Строка в формате ISO 8601.
        """
        return dt.isoformat()

    def _deserialize_datetime(self, value: str) -> datetime:
        """Десериализует ISO-строку обратно в datetime.

        Args:
            value: Строка в формате ISO 8601.

        Returns:
            Объект datetime с timezone UTC.
        """
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt

    def _row_to_raw_product(self, row: sqlite3.Row) -> RawProduct:
        """Преобразует строку БД в объект RawProduct.

        Args:
            row: Строка результата SQLite-запроса.

        Returns:
            Экземпляр RawProduct.
        """
        return RawProduct(
            avito_id=row["avito_id"],
            title=row["title"],
            price=row["price"],
            url=row["url"],
            description=row["description"],
            image_url=row["image_url"],
            seller_name=row["seller_name"],
            seller_rating=row["seller_rating"],
            seller_reviews=row["seller_reviews"],
            scraped_at=self._deserialize_datetime(row["scraped_at"]),
        )

    def _row_to_normalized_product(
        self, row: sqlite3.Row
    ) -> NormalizedProduct:
        """Преобразует строку БД в объект NormalizedProduct.

        Args:
            row: Строка результата SQLite-запроса.

        Returns:
            Экземпляр NormalizedProduct.
        """
        return NormalizedProduct(
            avito_id=row["avito_id"],
            original_title=row["original_title"],
            normalized_title=row["normalized_title"],
            product_category=row["product_category"],
            key_specs=row["key_specs"],
            price=row["price"],
            url=row["url"],
            full_url=row["full_url"],
            description=row["description"],
            image_url=row["image_url"],
            seller_name=row["seller_name"],
            seller_rating=row["seller_rating"],
            seller_reviews=row["seller_reviews"],
            scraped_at=self._deserialize_datetime(row["scraped_at"]),
            normalized_at=self._deserialize_datetime(row["normalized_at"]),
        )

    def save_raw_product(self, product: RawProduct) -> None:
        """Сохраняет один сырой товар (upsert по avito_id).

        Args:
            product: Сырой товар для сохранения.
        """
        conn = self._get_connection()

        sql = """
        INSERT INTO raw_products
            (avito_id, title, price, url, description,
             image_url, seller_name, seller_rating,
             seller_reviews, scraped_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(avito_id) DO UPDATE SET
            title = excluded.title,
            price = excluded.price,
            url = excluded.url,
            description = excluded.description,
            image_url = excluded.image_url,
            seller_name = excluded.seller_name,
            seller_rating = excluded.seller_rating,
            seller_reviews = excluded.seller_reviews,
            scraped_at = excluded.scraped_at
        """

        try:
            conn.execute(sql, (
                product.avito_id,
                product.title,
                product.price,
                product.url,
                product.description,
                product.image_url,
                product.seller_name,
                product.seller_rating,
                product.seller_reviews,
                self._serialize_datetime(product.scraped_at),
            ))
            conn.commit()

            logger.debug(
                "raw_product_saved",
                avito_id=product.avito_id,
                title=product.title,
            )
        except sqlite3.Error as e:
            logger.error(
                "raw_product_save_failed",
                exc_info=True,
                avito_id=product.avito_id,
                error=str(e),
            )
            raise

    def save_raw_products(self, products: list[RawProduct]) -> None:
        """Сохраняет список сырых товаров в одной транзакции.

        Args:
            products: Список сырых товаров.
        """
        if not products:
            return

        conn = self._get_connection()

        sql = """
        INSERT INTO raw_products
            (avito_id, title, price, url, description,
             image_url, seller_name, seller_rating,
             seller_reviews, scraped_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(avito_id) DO UPDATE SET
            title = excluded.title,
            price = excluded.price,
            url = excluded.url,
            description = excluded.description,
            image_url = excluded.image_url,
            seller_name = excluded.seller_name,
            seller_rating = excluded.seller_rating,
            seller_reviews = excluded.seller_reviews,
            scraped_at = excluded.scraped_at
        """

        rows = [
            (
                p.avito_id,
                p.title,
                p.price,
                p.url,
                p.description,
                p.image_url,
                p.seller_name,
                p.seller_rating,
                p.seller_reviews,
                self._serialize_datetime(p.scraped_at),
            )
            for p in products
        ]

        try:
            conn.executemany(sql, rows)
            conn.commit()

            logger.info(
                "raw_products_batch_saved",
                count=len(products),
            )
        except sqlite3.Error as e:
            logger.error(
                "raw_products_batch_save_failed",
                exc_info=True,
                count=len(products),
                error=str(e),
            )
            raise

    def get_all_raw_products(self) -> list[RawProduct]:
        """Возвращает все сырые товары.

        Returns:
            Список всех сырых товаров из таблицы raw_products.
        """
        conn = self._get_connection()

        try:
            cursor = conn.execute(
                "SELECT * FROM raw_products ORDER BY scraped_at DESC"
            )
            rows = cursor.fetchall()
            return [self._row_to_raw_product(row) for row in rows]
        except sqlite3.Error as e:
            logger.error(
                "raw_products_fetch_failed",
                exc_info=True,
                error=str(e),
            )
            raise

    def get_raw_products_without_normalization(self) -> list[RawProduct]:
        """Возвращает сырые товары без нормализации.

        Находит записи в raw_products, для которых нет
        соответствующей записи в normalized_products.

        Returns:
            Список ещё не нормализованных товаров.
        """
        conn = self._get_connection()

        sql = """
        SELECT r.* FROM raw_products r
        LEFT JOIN normalized_products n ON r.avito_id = n.avito_id
        WHERE n.avito_id IS NULL
        ORDER BY r.scraped_at DESC
        """

        try:
            cursor = conn.execute(sql)
            rows = cursor.fetchall()

            logger.info(
                "unnormalized_products_fetched",
                count=len(rows),
            )
            return [self._row_to_raw_product(row) for row in rows]
        except sqlite3.Error as e:
            logger.error(
                "unnormalized_products_fetch_failed",
                exc_info=True,
                error=str(e),
            )
            raise

    def save_normalized_product(self, product: NormalizedProduct) -> None:
        """Сохраняет один нормализованный товар (upsert по avito_id).

        Args:
            product: Нормализованный товар.
        """
        conn = self._get_connection()

        sql = """
        INSERT INTO normalized_products
            (avito_id, original_title, normalized_title,
             product_category, key_specs, price, url, full_url,
             description, image_url, seller_name, seller_rating,
             seller_reviews, scraped_at, normalized_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(avito_id) DO UPDATE SET
            original_title = excluded.original_title,
            normalized_title = excluded.normalized_title,
            product_category = excluded.product_category,
            key_specs = excluded.key_specs,
            price = excluded.price,
            url = excluded.url,
            full_url = excluded.full_url,
            description = excluded.description,
            image_url = excluded.image_url,
            seller_name = excluded.seller_name,
            seller_rating = excluded.seller_rating,
            seller_reviews = excluded.seller_reviews,
            scraped_at = excluded.scraped_at,
            normalized_at = excluded.normalized_at
        """

        try:
            conn.execute(sql, (
                product.avito_id,
                product.original_title,
                product.normalized_title,
                product.product_category,
                product.key_specs,
                product.price,
                product.url,
                product.full_url,
                product.description,
                product.image_url,
                product.seller_name,
                product.seller_rating,
                product.seller_reviews,
                self._serialize_datetime(product.scraped_at),
                self._serialize_datetime(product.normalized_at),
            ))
            conn.commit()

            logger.debug(
                "normalized_product_saved",
                avito_id=product.avito_id,
                normalized_title=product.normalized_title,
            )
        except sqlite3.Error as e:
            logger.error(
                "normalized_product_save_failed",
                exc_info=True,
                avito_id=product.avito_id,
                error=str(e),
            )
            raise

    def save_normalized_products(
        self, products: list[NormalizedProduct]
    ) -> None:
        """Сохраняет список нормализованных товаров в одной транзакции.

        Args:
            products: Список нормализованных товаров.
        """
        if not products:
            return

        conn = self._get_connection()

        sql = """
        INSERT INTO normalized_products
            (avito_id, original_title, normalized_title,
             product_category, key_specs, price, url, full_url,
             description, image_url, seller_name, seller_rating,
             seller_reviews, scraped_at, normalized_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(avito_id) DO UPDATE SET
            original_title = excluded.original_title,
            normalized_title = excluded.normalized_title,
            product_category = excluded.product_category,
            key_specs = excluded.key_specs,
            price = excluded.price,
            url = excluded.url,
            full_url = excluded.full_url,
            description = excluded.description,
            image_url = excluded.image_url,
            seller_name = excluded.seller_name,
            seller_rating = excluded.seller_rating,
            seller_reviews = excluded.seller_reviews,
            scraped_at = excluded.scraped_at,
            normalized_at = excluded.normalized_at
        """

        rows = [
            (
                p.avito_id,
                p.original_title,
                p.normalized_title,
                p.product_category,
                p.key_specs,
                p.price,
                p.url,
                p.full_url,
                p.description,
                p.image_url,
                p.seller_name,
                p.seller_rating,
                p.seller_reviews,
                self._serialize_datetime(p.scraped_at),
                self._serialize_datetime(p.normalized_at),
            )
            for p in products
        ]

        try:
            conn.executemany(sql, rows)
            conn.commit()

            logger.info(
                "normalized_products_batch_saved",
                count=len(products),
            )
        except sqlite3.Error as e:
            logger.error(
                "normalized_products_batch_save_failed",
                exc_info=True,
                count=len(products),
                error=str(e),
            )
            raise

    def get_all_normalized_products(self) -> list[NormalizedProduct]:
        """Возвращает все нормализованные товары.

        Returns:
            Список нормализованных товаров, отсортированных по названию.
        """
        conn = self._get_connection()

        try:
            cursor = conn.execute(
                "SELECT * FROM normalized_products "
                "ORDER BY normalized_title, price"
            )
            rows = cursor.fetchall()
            return [self._row_to_normalized_product(row) for row in rows]
        except sqlite3.Error as e:
            logger.error(
                "normalized_products_fetch_failed",
                exc_info=True,
                error=str(e),
            )
            raise

    def raw_product_exists(self, avito_id: str) -> bool:
        """Проверяет существование сырого товара по ID.

        Args:
            avito_id: Идентификатор объявления Avito.

        Returns:
            True если товар найден в таблице raw_products.
        """
        conn = self._get_connection()

        try:
            cursor = conn.execute(
                "SELECT 1 FROM raw_products WHERE avito_id = ? LIMIT 1",
                (avito_id,),
            )
            return cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(
                "raw_product_exists_check_failed",
                exc_info=True,
                avito_id=avito_id,
                error=str(e),
            )
            raise

    def get_raw_products_count(self) -> int:
        """Возвращает количество сырых товаров.

        Returns:
            Число записей в таблице raw_products.
        """
        conn = self._get_connection()

        try:
            cursor = conn.execute(
                "SELECT COUNT(*) as cnt FROM raw_products"
            )
            row = cursor.fetchone()
            return row["cnt"] if row else 0
        except sqlite3.Error as e:
            logger.error(
                "raw_products_count_failed",
                exc_info=True,
                error=str(e),
            )
            raise

    def get_normalized_products_count(self) -> int:
        """Возвращает количество нормализованных товаров.

        Returns:
            Число записей в таблице normalized_products.
        """
        conn = self._get_connection()

        try:
            cursor = conn.execute(
                "SELECT COUNT(*) as cnt FROM normalized_products"
            )
            row = cursor.fetchone()
            return row["cnt"] if row else 0
        except sqlite3.Error as e:
            logger.error(
                "normalized_products_count_failed",
                exc_info=True,
                error=str(e),
            )
            raise

    def close(self) -> None:
        """Закрывает соединение с базой данных."""
        if self._connection is not None:
            try:
                self._connection.close()
                self._connection = None
                logger.info("database_closed", db_path=self._db_path)
            except sqlite3.Error as e:
                logger.error(
                    "database_close_failed",
                    exc_info=True,
                    error=str(e),
                )
