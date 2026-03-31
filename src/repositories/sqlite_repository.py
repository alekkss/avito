"""SQLite-реализация репозитория объявлений аренды.

Хранит объявления краткосрочной аренды в таблице SQLite.
Поддерживает upsert-логику (вставка или обновление по external_id),
батчевое сохранение и сериализацию массивов/словарей в JSON.
"""

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.config import get_logger
from src.models import RawListing, RoomCategory
from src.repositories.base import BaseListingRepository

logger = get_logger("sqlite_repository")


class SQLiteListingRepository(BaseListingRepository):
    """Репозиторий объявлений аренды на базе SQLite.

    Создаёт файл базы данных по указанному пути, автоматически
    создаёт необходимые таблицы при инициализации. Массивы
    (price_60_days, calendar_60_days) и словари (события,
    analytics_payload) сериализуются в JSON-строки.

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

        create_listings_table = """
        CREATE TABLE IF NOT EXISTS listings (
            external_id          TEXT PRIMARY KEY,
            latitude             REAL NOT NULL DEFAULT 0.0,
            longitude            REAL NOT NULL DEFAULT 0.0,
            room_category        TEXT NOT NULL DEFAULT 'Неизвестно',
            price_60_days        TEXT NOT NULL DEFAULT '[]',
            calendar_60_days     TEXT NOT NULL DEFAULT '[]',
            snapshot_timestamp   TEXT NOT NULL,
            last_host_update     TEXT,
            min_stay             INTEGER NOT NULL DEFAULT 1,
            is_instant_book      INTEGER NOT NULL DEFAULT 0,
            host_rating          REAL NOT NULL DEFAULT 0.0,
            price_change_event   TEXT,
            booking_block_event  TEXT,
            cancellation_event   TEXT,
            analytics_payload    TEXT,
            url                  TEXT DEFAULT '',
            title                TEXT DEFAULT ''
        )
        """

        create_index_room_category = """
        CREATE INDEX IF NOT EXISTS idx_room_category
        ON listings (room_category)
        """

        create_index_snapshot = """
        CREATE INDEX IF NOT EXISTS idx_snapshot_timestamp
        ON listings (snapshot_timestamp)
        """

        create_index_coordinates = """
        CREATE INDEX IF NOT EXISTS idx_coordinates
        ON listings (latitude, longitude)
        """

        try:
            conn.execute(create_listings_table)
            conn.execute(create_index_room_category)
            conn.execute(create_index_snapshot)
            conn.execute(create_index_coordinates)
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

    def _deserialize_datetime(self, value: str | None) -> datetime | None:
        """Десериализует ISO-строку обратно в datetime.

        Args:
            value: Строка в формате ISO 8601 или None.

        Returns:
            Объект datetime с timezone UTC или None.
        """
        if value is None:
            return None
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt

    def _serialize_json(self, value: Any) -> str | None:
        """Сериализует объект Python в JSON-строку.

        Args:
            value: Список, словарь или None.

        Returns:
            JSON-строка или None если значение None.
        """
        if value is None:
            return None
        return json.dumps(value, ensure_ascii=False)

    def _deserialize_json_list(self, value: str | None) -> list[int]:
        """Десериализует JSON-строку в список целых чисел.

        Args:
            value: JSON-строка с массивом или None.

        Returns:
            Список целых чисел. Пустой список если значение None.
        """
        if value is None:
            return []
        try:
            result = json.loads(value)
            if isinstance(result, list):
                return [int(x) for x in result]
            return []
        except (json.JSONDecodeError, ValueError):
            return []

    def _deserialize_json_dict(
        self, value: str | None
    ) -> dict[str, Any] | None:
        """Десериализует JSON-строку в словарь.

        Args:
            value: JSON-строка со словарём или None.

        Returns:
            Словарь или None если значение None или невалидный JSON.
        """
        if value is None:
            return None
        try:
            result = json.loads(value)
            if isinstance(result, dict):
                return result
            return None
        except json.JSONDecodeError:
            return None

    def _row_to_listing(self, row: sqlite3.Row) -> RawListing:
        """Преобразует строку БД в объект RawListing.

        Args:
            row: Строка результата SQLite-запроса.

        Returns:
            Экземпляр RawListing.
        """
        snapshot_dt = self._deserialize_datetime(row["snapshot_timestamp"])
        if snapshot_dt is None:
            snapshot_dt = datetime.now(timezone.utc)

        return RawListing(
            external_id=row["external_id"],
            latitude=float(row["latitude"]),
            longitude=float(row["longitude"]),
            room_category=RoomCategory(row["room_category"]),
            price_60_days=self._deserialize_json_list(row["price_60_days"]),
            calendar_60_days=self._deserialize_json_list(
                row["calendar_60_days"]
            ),
            snapshot_timestamp=snapshot_dt,
            last_host_update=self._deserialize_datetime(
                row["last_host_update"]
            ),
            min_stay=int(row["min_stay"]),
            is_instant_book=bool(row["is_instant_book"]),
            host_rating=float(row["host_rating"]),
            price_change_event=self._deserialize_json_dict(
                row["price_change_event"]
            ),
            booking_block_event=self._deserialize_json_dict(
                row["booking_block_event"]
            ),
            cancellation_event=self._deserialize_json_dict(
                row["cancellation_event"]
            ),
            analytics_payload=self._deserialize_json_dict(
                row["analytics_payload"]
            ),
            url=row["url"],
            title=row["title"],
        )

    def _listing_to_params(self, listing: RawListing) -> tuple:
        """Преобразует RawListing в кортеж параметров для SQL-запроса.

        Args:
            listing: Объявление аренды.

        Returns:
            Кортеж значений в порядке столбцов таблицы listings.
        """
        return (
            listing.external_id,
            listing.latitude,
            listing.longitude,
            listing.room_category.value,
            self._serialize_json(listing.price_60_days),
            self._serialize_json(listing.calendar_60_days),
            self._serialize_datetime(listing.snapshot_timestamp),
            self._serialize_datetime(listing.last_host_update)
            if listing.last_host_update
            else None,
            listing.min_stay,
            1 if listing.is_instant_book else 0,
            listing.host_rating,
            self._serialize_json(listing.price_change_event),
            self._serialize_json(listing.booking_block_event),
            self._serialize_json(listing.cancellation_event),
            self._serialize_json(listing.analytics_payload),
            listing.url,
            listing.title,
        )

    def save_listing(self, listing: RawListing) -> None:
        """Сохраняет одно объявление (upsert по external_id).

        Args:
            listing: Объявление аренды для сохранения.
        """
        conn = self._get_connection()

        sql = """
        INSERT INTO listings
            (external_id, latitude, longitude, room_category,
             price_60_days, calendar_60_days, snapshot_timestamp,
             last_host_update, min_stay, is_instant_book, host_rating,
             price_change_event, booking_block_event, cancellation_event,
             analytics_payload, url, title)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(external_id) DO UPDATE SET
            latitude = excluded.latitude,
            longitude = excluded.longitude,
            room_category = excluded.room_category,
            price_60_days = excluded.price_60_days,
            calendar_60_days = excluded.calendar_60_days,
            snapshot_timestamp = excluded.snapshot_timestamp,
            last_host_update = excluded.last_host_update,
            min_stay = excluded.min_stay,
            is_instant_book = excluded.is_instant_book,
            host_rating = excluded.host_rating,
            price_change_event = excluded.price_change_event,
            booking_block_event = excluded.booking_block_event,
            cancellation_event = excluded.cancellation_event,
            analytics_payload = excluded.analytics_payload,
            url = excluded.url,
            title = excluded.title
        """

        try:
            conn.execute(sql, self._listing_to_params(listing))
            conn.commit()

            logger.debug(
                "listing_saved",
                external_id=listing.external_id,
                title=listing.title[:50],
            )
        except sqlite3.Error as e:
            logger.error(
                "listing_save_failed",
                exc_info=True,
                external_id=listing.external_id,
                error=str(e),
            )
            raise

    def save_listings(self, listings: list[RawListing]) -> None:
        """Сохраняет список объявлений в одной транзакции.

        Args:
            listings: Список объявлений аренды.
        """
        if not listings:
            return

        conn = self._get_connection()

        sql = """
        INSERT INTO listings
            (external_id, latitude, longitude, room_category,
             price_60_days, calendar_60_days, snapshot_timestamp,
             last_host_update, min_stay, is_instant_book, host_rating,
             price_change_event, booking_block_event, cancellation_event,
             analytics_payload, url, title)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(external_id) DO UPDATE SET
            latitude = excluded.latitude,
            longitude = excluded.longitude,
            room_category = excluded.room_category,
            price_60_days = excluded.price_60_days,
            calendar_60_days = excluded.calendar_60_days,
            snapshot_timestamp = excluded.snapshot_timestamp,
            last_host_update = excluded.last_host_update,
            min_stay = excluded.min_stay,
            is_instant_book = excluded.is_instant_book,
            host_rating = excluded.host_rating,
            price_change_event = excluded.price_change_event,
            booking_block_event = excluded.booking_block_event,
            cancellation_event = excluded.cancellation_event,
            analytics_payload = excluded.analytics_payload,
            url = excluded.url,
            title = excluded.title
        """

        rows = [self._listing_to_params(listing) for listing in listings]

        try:
            conn.executemany(sql, rows)
            conn.commit()

            logger.info(
                "listings_batch_saved",
                count=len(listings),
            )
        except sqlite3.Error as e:
            logger.error(
                "listings_batch_save_failed",
                exc_info=True,
                count=len(listings),
                error=str(e),
            )
            raise

    def get_all_listings(self) -> list[RawListing]:
        """Возвращает все объявления аренды.

        Returns:
            Список всех объявлений, отсортированных по дате снимка.
        """
        conn = self._get_connection()

        try:
            cursor = conn.execute(
                "SELECT * FROM listings ORDER BY snapshot_timestamp DESC"
            )
            rows = cursor.fetchall()
            return [self._row_to_listing(row) for row in rows]
        except sqlite3.Error as e:
            logger.error(
                "listings_fetch_failed",
                exc_info=True,
                error=str(e),
            )
            raise

    def listing_exists(self, external_id: str) -> bool:
        """Проверяет существование объявления по ID.

        Args:
            external_id: Идентификатор объявления (например, "av_123").

        Returns:
            True если объявление найдено в таблице listings.
        """
        conn = self._get_connection()

        try:
            cursor = conn.execute(
                "SELECT 1 FROM listings WHERE external_id = ? LIMIT 1",
                (external_id,),
            )
            return cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(
                "listing_exists_check_failed",
                exc_info=True,
                external_id=external_id,
                error=str(e),
            )
            raise

    def get_listings_count(self) -> int:
        """Возвращает количество объявлений в хранилище.

        Returns:
            Число записей в таблице listings.
        """
        conn = self._get_connection()

        try:
            cursor = conn.execute(
                "SELECT COUNT(*) as cnt FROM listings"
            )
            row = cursor.fetchone()
            return row["cnt"] if row else 0
        except sqlite3.Error as e:
            logger.error(
                "listings_count_failed",
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
