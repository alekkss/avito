"""Трекер здоровья прокси-серверов.

Отслеживает статистику успехов и неудач каждого прокси,
автоматически исключает из пула прокси с высоким
процентом банов. Предоставляет метод выбора следующего
здорового прокси, пропуская «мёртвые».

Паттерн Strategy: трекер можно заменить на альтернативную
реализацию (например, с весовой ротацией или внешним API
проверки прокси) без изменения BrowserService.
"""

from dataclasses import dataclass, field
from enum import Enum

from src.config import get_logger

logger = get_logger("proxy_health")


class ProxyStatus(Enum):
    """Статус прокси в пуле.

    Attributes:
        HEALTHY: Прокси работает нормально.
        SUSPECT: Прокси получил баны, но ещё не исключён.
        DEAD: Прокси исключён из ротации (слишком много банов подряд).
    """

    HEALTHY = "healthy"
    SUSPECT = "suspect"
    DEAD = "dead"


# Порог последовательных банов для пометки прокси как DEAD.
# После 3 банов подряд прокси исключается из ротации —
# дальнейшие попытки с ним будут тратить время впустую.
DEFAULT_MAX_CONSECUTIVE_BANS: int = 3

# Порог последовательных ошибок соединения (ERR_TUNNEL и т.д.)
# для пометки прокси как DEAD. Ошибки соединения означают,
# что прокси-сервер не отвечает — нет смысла повторять.
DEFAULT_MAX_CONSECUTIVE_CONN_ERRORS: int = 2


@dataclass
class ProxyStats:
    """Статистика одного прокси-сервера.

    Attributes:
        server: URL прокси (для логирования).
        total_successes: Общее количество успешных запросов.
        total_bans: Общее количество банов.
        total_conn_errors: Общее количество ошибок соединения.
        consecutive_bans: Текущая серия банов подряд
            (сбрасывается при успехе).
        consecutive_conn_errors: Текущая серия ошибок соединения подряд
            (сбрасывается при успехе).
        status: Текущий статус прокси в пуле.
    """

    server: str
    total_successes: int = 0
    total_bans: int = 0
    total_conn_errors: int = 0
    consecutive_bans: int = 0
    consecutive_conn_errors: int = 0
    status: ProxyStatus = ProxyStatus.HEALTHY


class ProxyHealthTracker:
    """Трекер здоровья пула прокси-серверов.

    Управляет статистикой каждого прокси и предоставляет метод
    выбора следующего здорового прокси из пула. Автоматически
    исключает прокси с высоким процентом последовательных
    неудач (банов или ошибок соединения).

    Принцип работы:
    - При каждом успехе (report_success) серия банов и ошибок
      сбрасывается → прокси остаётся/возвращается в HEALTHY.
    - При бане (report_ban) серия банов увеличивается. Если
      серия достигла порога — прокси помечается как DEAD.
    - При ошибке соединения (report_connection_error) серия
      ошибок увеличивается. Если серия достигла порога —
      прокси помечается как DEAD.
    - get_next_healthy() перебирает пул циклически, пропуская
      DEAD-прокси. Если все прокси мертвы — возвращает None.

    Attributes:
        _stats: Словарь статистик по серверу прокси.
        _servers_order: Упорядоченный список серверов для циклического
            перебора (порядок соответствует порядку регистрации).
        _current_index: Текущий индекс в циклическом переборе.
        _max_consecutive_bans: Порог серии банов для исключения.
        _max_consecutive_conn_errors: Порог серии ошибок соединения.
    """

    def __init__(
        self,
        max_consecutive_bans: int = DEFAULT_MAX_CONSECUTIVE_BANS,
        max_consecutive_conn_errors: int = DEFAULT_MAX_CONSECUTIVE_CONN_ERRORS,
    ) -> None:
        """Инициализирует трекер.

        Args:
            max_consecutive_bans: Сколько банов подряд допустимо,
                прежде чем прокси будет исключён из пула.
            max_consecutive_conn_errors: Сколько ошибок соединения подряд
                допустимо, прежде чем прокси будет исключён.
        """
        self._stats: dict[str, ProxyStats] = {}
        self._servers_order: list[str] = []
        self._current_index: int = -1
        self._max_consecutive_bans = max_consecutive_bans
        self._max_consecutive_conn_errors = max_consecutive_conn_errors

    def register(self, server: str) -> None:
        """Регистрирует прокси в трекере.

        Если прокси уже зарегистрирован — игнорируется.
        Порядок регистрации определяет порядок циклического
        перебора в get_next_healthy().

        Args:
            server: URL прокси-сервера (например, "http://1.2.3.4:8080").
        """
        if server in self._stats:
            return

        self._stats[server] = ProxyStats(server=server)
        self._servers_order.append(server)

        logger.debug(
            "proxy_registered",
            server=server,
            total_registered=len(self._servers_order),
        )

    def register_many(self, servers: list[str]) -> None:
        """Регистрирует список прокси в трекере.

        Args:
            servers: Список URL прокси-серверов.
        """
        for server in servers:
            self.register(server)

        logger.info(
            "proxies_registered_in_tracker",
            total=len(self._servers_order),
        )

    def report_success(self, server: str) -> None:
        """Регистрирует успешный запрос через прокси.

        Сбрасывает серию банов и ошибок соединения. Если прокси
        был в статусе SUSPECT — возвращает в HEALTHY.

        Args:
            server: URL прокси-сервера.
        """
        stats = self._stats.get(server)
        if stats is None:
            return

        stats.total_successes += 1
        stats.consecutive_bans = 0
        stats.consecutive_conn_errors = 0

        if stats.status == ProxyStatus.SUSPECT:
            stats.status = ProxyStatus.HEALTHY
            logger.info(
                "proxy_recovered_to_healthy",
                server=server,
                total_successes=stats.total_successes,
            )

    def report_ban(self, server: str) -> None:
        """Регистрирует бан (блокировку Avito) через прокси.

        Увеличивает серию банов. При достижении порога —
        помечает прокси как DEAD и логирует исключение.

        Args:
            server: URL прокси-сервера.
        """
        stats = self._stats.get(server)
        if stats is None:
            return

        stats.total_bans += 1
        stats.consecutive_bans += 1

        if stats.consecutive_bans >= self._max_consecutive_bans:
            if stats.status != ProxyStatus.DEAD:
                stats.status = ProxyStatus.DEAD

                alive = self.alive_count
                total = len(self._servers_order)

                logger.warning(
                    "proxy_marked_dead_by_bans",
                    server=server,
                    consecutive_bans=stats.consecutive_bans,
                    threshold=self._max_consecutive_bans,
                    total_bans=stats.total_bans,
                    alive_proxies=alive,
                    total_proxies=total,
                )
                print(
                    f"  [прокси-здоровье] Прокси {server} исключён "
                    f"из пула ({stats.consecutive_bans} банов подряд). "
                    f"Живых: {alive}/{total}"
                )
        elif stats.status == ProxyStatus.HEALTHY:
            stats.status = ProxyStatus.SUSPECT
            logger.info(
                "proxy_suspect_after_ban",
                server=server,
                consecutive_bans=stats.consecutive_bans,
                threshold=self._max_consecutive_bans,
            )

    def report_connection_error(self, server: str) -> None:
        """Регистрирует ошибку соединения через прокси.

        Ошибки соединения (ERR_TUNNEL_CONNECTION_FAILED и т.д.)
        означают, что прокси-сервер не отвечает. Порог исключения
        ниже, чем для банов, т.к. такие ошибки однозначнее.

        Args:
            server: URL прокси-сервера.
        """
        stats = self._stats.get(server)
        if stats is None:
            return

        stats.total_conn_errors += 1
        stats.consecutive_conn_errors += 1

        if stats.consecutive_conn_errors >= self._max_consecutive_conn_errors:
            if stats.status != ProxyStatus.DEAD:
                stats.status = ProxyStatus.DEAD

                alive = self.alive_count
                total = len(self._servers_order)

                logger.warning(
                    "proxy_marked_dead_by_conn_errors",
                    server=server,
                    consecutive_conn_errors=stats.consecutive_conn_errors,
                    threshold=self._max_consecutive_conn_errors,
                    total_conn_errors=stats.total_conn_errors,
                    alive_proxies=alive,
                    total_proxies=total,
                )
                print(
                    f"  [прокси-здоровье] Прокси {server} исключён "
                    f"из пула ({stats.consecutive_conn_errors} ошибок "
                    f"соединения подряд). Живых: {alive}/{total}"
                )

    def get_next_healthy(self) -> str | None:
        """Возвращает URL следующего здорового прокси из пула.

        Циклически перебирает зарегистрированные прокси, пропуская
        те, которые помечены как DEAD. Если все прокси мертвы —
        возвращает None.

        Returns:
            URL прокси-сервера или None если живых прокси нет.
        """
        total = len(self._servers_order)
        if total == 0:
            return None

        # Перебираем не более total прокси (полный круг)
        for _ in range(total):
            self._current_index = (self._current_index + 1) % total
            server = self._servers_order[self._current_index]
            stats = self._stats[server]

            if stats.status != ProxyStatus.DEAD:
                return server

        # Все прокси мертвы
        logger.error(
            "all_proxies_dead",
            total=total,
            dead_count=total,
        )
        print(
            f"  [прокси-здоровье] ⚠️  ВСЕ прокси исключены из "
            f"пула ({total} шт.). Парсинг невозможен."
        )
        return None

    def is_dead(self, server: str) -> bool:
        """Проверяет, исключён ли прокси из пула.

        Args:
            server: URL прокси-сервера.

        Returns:
            True если прокси помечен как DEAD.
        """
        stats = self._stats.get(server)
        if stats is None:
            return False
        return stats.status == ProxyStatus.DEAD

    @property
    def alive_count(self) -> int:
        """Количество живых (не DEAD) прокси в пуле.

        Returns:
            Число прокси со статусом HEALTHY или SUSPECT.
        """
        return sum(
            1 for stats in self._stats.values()
            if stats.status != ProxyStatus.DEAD
        )

    @property
    def total_count(self) -> int:
        """Общее количество зарегистрированных прокси.

        Returns:
            Размер пула.
        """
        return len(self._servers_order)

    def get_stats_summary(self) -> dict[str, dict[str, object]]:
        """Возвращает сводку статистики по всем прокси.

        Используется для финального отчёта и отладки.

        Returns:
            Словарь: server → {status, successes, bans, conn_errors, ...}.
        """
        summary: dict[str, dict[str, object]] = {}
        for server, stats in self._stats.items():
            summary[server] = {
                "status": stats.status.value,
                "total_successes": stats.total_successes,
                "total_bans": stats.total_bans,
                "total_conn_errors": stats.total_conn_errors,
                "consecutive_bans": stats.consecutive_bans,
                "consecutive_conn_errors": stats.consecutive_conn_errors,
            }
        return summary

    def log_summary(self) -> None:
        """Логирует финальную сводку здоровья пула прокси.

        Вызывается в конце пайплайна для отчёта.
        """
        alive = self.alive_count
        total = self.total_count
        dead = total - alive

        logger.info(
            "proxy_health_summary",
            total=total,
            alive=alive,
            dead=dead,
        )

        for server, stats in self._stats.items():
            logger.info(
                "proxy_stats",
                server=server,
                status=stats.status.value,
                successes=stats.total_successes,
                bans=stats.total_bans,
                conn_errors=stats.total_conn_errors,
                consecutive_bans=stats.consecutive_bans,
            )
