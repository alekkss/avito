"""Трекер здоровья прокси-серверов.

Отслеживает статистику успехов и неудач каждого прокси,
автоматически исключает из пула прокси с высоким
процентом банов. Предоставляет метод выбора следующего
здорового прокси, пропуская «мёртвые».

Учитывает подсети (/24): если несколько прокси из одной
подсети забанены — превентивно исключает оставшиеся прокси
из этой подсети. Avito банит целыми подсетями, поэтому
нет смысла пробовать соседние IP после 2+ банов в подсети.

Паттерн Strategy: трекер можно заменить на альтернативную
реализацию (например, с весовой ротацией или внешним API
проверки прокси) без изменения BrowserService.
"""

from dataclasses import dataclass, field
from enum import Enum
from urllib.parse import urlparse

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

# Порог мёртвых прокси в подсети /24 для превентивного исключения.
# Если в подсети 2+ прокси помечены как DEAD — все оставшиеся
# живые прокси из этой подсети тоже помечаются как DEAD.
# Avito банит целыми подсетями: после 2 банов вероятность
# работоспособности соседних IP в той же /24 близка к нулю.
DEFAULT_SUBNET_DEAD_THRESHOLD: int = 2


def _extract_subnet(server: str) -> str:
    """Извлекает подсеть /24 из URL прокси-сервера.

    Парсит URL, извлекает IP-адрес хоста и возвращает
    первые три октета (подсеть /24). Для не-IP хостов
    (домены) возвращает сам хост как «подсеть».

    Args:
        server: URL прокси (например, "http://194.28.193.153:11223").

    Returns:
        Строка подсети (например, "194.28.193").
    """
    try:
        parsed = urlparse(server)
        host = parsed.hostname or ""

        # Проверяем, что это IPv4-адрес
        parts = host.split(".")
        if len(parts) == 4 and all(p.isdigit() for p in parts):
            # Возвращаем первые 3 октета как подсеть /24
            return f"{parts[0]}.{parts[1]}.{parts[2]}"

        # Для доменных имён или нестандартных хостов —
        # возвращаем хост целиком (каждый домен = отдельная «подсеть»)
        return host
    except Exception:
        return server


@dataclass
class ProxyStats:
    """Статистика одного прокси-сервера.

    Attributes:
        server: URL прокси (для логирования).
        subnet: Подсеть /24 прокси (первые 3 октета IP).
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
    subnet: str = ""
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

    Учитывает подсети /24: при бане прокси проверяет, сколько
    прокси из той же подсети уже мертвы. Если порог превышен —
    превентивно исключает все оставшиеся прокси из этой подсети.
    Это устраняет бесполезные попытки на IP из забаненной подсети
    и экономит десятки секунд на карточку.

    Принцип работы:
    - При каждом успехе (report_success) серия банов и ошибок
      сбрасывается → прокси остаётся/возвращается в HEALTHY.
    - При бане (report_ban) серия банов увеличивается. Если
      серия достигла порога — прокси помечается как DEAD.
      Дополнительно проверяется подсеть: если в подсети уже
      достаточно мёртвых — остальные прокси из неё тоже
      исключаются.
    - При ошибке соединения (report_connection_error) серия
      ошибок увеличивается. Если серия достигла порога —
      прокси помечается как DEAD + проверка подсети.
    - get_next_healthy() перебирает пул циклически, пропуская
      DEAD-прокси. При наличии выбора предпочитает прокси из
      «чистых» подсетей (без банов). Если все прокси мертвы —
      возвращает None.

    Attributes:
        _stats: Словарь статистик по серверу прокси.
        _servers_order: Упорядоченный список серверов для циклического
            перебора (порядок соответствует порядку регистрации).
        _current_index: Текущий индекс в циклическом переборе.
        _max_consecutive_bans: Порог серии банов для исключения.
        _max_consecutive_conn_errors: Порог серии ошибок соединения.
        _subnet_dead_threshold: Порог мёртвых прокси в подсети
            для превентивного исключения остальных.
        _subnet_servers: Словарь подсеть → список серверов в ней.
    """

    def __init__(
        self,
        max_consecutive_bans: int = DEFAULT_MAX_CONSECUTIVE_BANS,
        max_consecutive_conn_errors: int = DEFAULT_MAX_CONSECUTIVE_CONN_ERRORS,
        subnet_dead_threshold: int = DEFAULT_SUBNET_DEAD_THRESHOLD,
    ) -> None:
        """Инициализирует трекер.

        Args:
            max_consecutive_bans: Сколько банов подряд допустимо,
                прежде чем прокси будет исключён из пула.
            max_consecutive_conn_errors: Сколько ошибок соединения подряд
                допустимо, прежде чем прокси будет исключён.
            subnet_dead_threshold: Сколько мёртвых прокси в одной /24
                подсети допустимо, прежде чем остальные из этой подсети
                будут превентивно исключены.
        """
        self._stats: dict[str, ProxyStats] = {}
        self._servers_order: list[str] = []
        self._current_index: int = -1
        self._max_consecutive_bans = max_consecutive_bans
        self._max_consecutive_conn_errors = max_consecutive_conn_errors
        self._subnet_dead_threshold = subnet_dead_threshold
        self._subnet_servers: dict[str, list[str]] = {}

    def register(self, server: str) -> None:
        """Регистрирует прокси в трекере.

        Если прокси уже зарегистрирован — игнорируется.
        Порядок регистрации определяет порядок циклического
        перебора в get_next_healthy(). При регистрации
        автоматически определяется подсеть /24.

        Args:
            server: URL прокси-сервера (например, "http://1.2.3.4:8080").
        """
        if server in self._stats:
            return

        subnet = _extract_subnet(server)

        self._stats[server] = ProxyStats(
            server=server,
            subnet=subnet,
        )
        self._servers_order.append(server)

        # Регистрируем сервер в индексе подсетей
        if subnet not in self._subnet_servers:
            self._subnet_servers[subnet] = []
        self._subnet_servers[subnet].append(server)

        logger.debug(
            "proxy_registered",
            server=server,
            subnet=subnet,
            total_registered=len(self._servers_order),
            subnet_size=len(self._subnet_servers[subnet]),
        )

    def register_many(self, servers: list[str]) -> None:
        """Регистрирует список прокси в трекере.

        Args:
            servers: Список URL прокси-серверов.
        """
        for server in servers:
            self.register(server)

        # Логируем распределение по подсетям
        subnet_summary = {
            subnet: len(srvs)
            for subnet, srvs in self._subnet_servers.items()
        }

        logger.info(
            "proxies_registered_in_tracker",
            total=len(self._servers_order),
            subnets=len(self._subnet_servers),
            subnet_distribution=subnet_summary,
        )

        # Предупреждаем о подсетях с большим количеством прокси —
        # они уязвимы к массовому бану
        for subnet, count in subnet_summary.items():
            if count >= 3:
                logger.warning(
                    "subnet_high_concentration",
                    subnet=f"{subnet}.0/24",
                    proxy_count=count,
                    risk="Avito может забанить всю подсеть разом",
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
                subnet=stats.subnet,
                total_successes=stats.total_successes,
            )

    def report_ban(self, server: str) -> None:
        """Регистрирует бан (блокировку Avito) через прокси.

        Увеличивает серию банов. При достижении порога —
        помечает прокси как DEAD и логирует исключение.
        Дополнительно проверяет подсеть: если в ней уже
        достаточно мёртвых прокси — превентивно исключает
        оставшиеся.

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
                    subnet=stats.subnet,
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

                # Проверяем подсеть на массовый бан
                self._check_subnet_health(stats.subnet)

        elif stats.status == ProxyStatus.HEALTHY:
            stats.status = ProxyStatus.SUSPECT
            logger.info(
                "proxy_suspect_after_ban",
                server=server,
                subnet=stats.subnet,
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
                    subnet=stats.subnet,
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

                # Проверяем подсеть
                self._check_subnet_health(stats.subnet)

    def _check_subnet_health(self, subnet: str) -> None:
        """Проверяет здоровье подсети и превентивно исключает прокси.

        Если количество DEAD-прокси в подсети достигло порога
        (subnet_dead_threshold) — все оставшиеся живые прокси
        из этой подсети помечаются как DEAD. Avito банит целыми
        подсетями /24, поэтому нет смысла пробовать соседние IP
        после нескольких банов в той же подсети.

        Args:
            subnet: Подсеть /24 для проверки (первые 3 октета).
        """
        servers_in_subnet = self._subnet_servers.get(subnet, [])

        if len(servers_in_subnet) <= 1:
            # Одиночный прокси в подсети — нет смысла проверять
            return

        # Считаем мёртвых в подсети
        dead_in_subnet = sum(
            1 for srv in servers_in_subnet
            if self._stats[srv].status == ProxyStatus.DEAD
        )

        if dead_in_subnet < self._subnet_dead_threshold:
            return

        # Порог превышен — превентивно исключаем всех живых из подсети
        alive_in_subnet = [
            srv for srv in servers_in_subnet
            if self._stats[srv].status != ProxyStatus.DEAD
        ]

        if not alive_in_subnet:
            return

        logger.warning(
            "subnet_mass_ban_detected",
            subnet=f"{subnet}.0/24",
            dead_count=dead_in_subnet,
            threshold=self._subnet_dead_threshold,
            total_in_subnet=len(servers_in_subnet),
            preemptively_killing=len(alive_in_subnet),
        )
        print(
            f"  [прокси-здоровье] ⚠️  Подсеть {subnet}.0/24 "
            f"забанена ({dead_in_subnet} мёртвых из "
            f"{len(servers_in_subnet)}). "
            f"Превентивно исключаю ещё {len(alive_in_subnet)} "
            f"прокси из этой подсети."
        )

        for srv in alive_in_subnet:
            srv_stats = self._stats[srv]
            srv_stats.status = ProxyStatus.DEAD

            logger.info(
                "proxy_killed_by_subnet_ban",
                server=srv,
                subnet=f"{subnet}.0/24",
                previous_status=srv_stats.status.value
                if srv_stats.status != ProxyStatus.DEAD
                else "dead",
                dead_in_subnet=dead_in_subnet,
                threshold=self._subnet_dead_threshold,
            )

        alive_total = self.alive_count
        total = len(self._servers_order)

        logger.info(
            "subnet_cleanup_completed",
            subnet=f"{subnet}.0/24",
            killed=len(alive_in_subnet),
            alive_remaining=alive_total,
            total_proxies=total,
        )
        print(
            f"  [прокси-здоровье] После очистки подсети: "
            f"живых {alive_total}/{total}"
        )

    def get_next_healthy(self) -> str | None:
        """Возвращает URL следующего здорового прокси из пула.

        Циклически перебирает зарегистрированные прокси, пропуская
        те, которые помечены как DEAD. При наличии нескольких
        кандидатов предпочитает прокси из «чистых» подсетей
        (без банов) — это снижает вероятность попадания на
        забаненную подсеть.

        Returns:
            URL прокси-сервера или None если живых прокси нет.
        """
        total = len(self._servers_order)
        if total == 0:
            return None

        # Собираем всех живых кандидатов за один проход
        candidates: list[str] = []
        clean_candidates: list[str] = []

        for _ in range(total):
            self._current_index = (self._current_index + 1) % total
            server = self._servers_order[self._current_index]
            stats = self._stats[server]

            if stats.status == ProxyStatus.DEAD:
                continue

            candidates.append(server)

            # «Чистый» кандидат — из подсети без банов
            subnet = stats.subnet
            subnet_has_bans = any(
                self._stats[srv].total_bans > 0
                for srv in self._subnet_servers.get(subnet, [])
                if srv != server
            )
            if not subnet_has_bans:
                clean_candidates.append(server)

            # Достаточно первого кандидата для базового выбора
            # (сохраняем циклический порядок), но проверяем,
            # можно ли найти «чистого» кандидата
            if candidates and clean_candidates:
                # Нашли чистого — используем его
                break

        if clean_candidates:
            chosen = clean_candidates[0]
            # Устанавливаем _current_index на выбранный сервер
            # для корректного продолжения циклического перебора
            self._current_index = self._servers_order.index(chosen)
            return chosen

        if candidates:
            chosen = candidates[0]
            self._current_index = self._servers_order.index(chosen)
            return chosen

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
            Словарь: server → {status, successes, bans, conn_errors, subnet, ...}.
        """
        summary: dict[str, dict[str, object]] = {}
        for server, stats in self._stats.items():
            summary[server] = {
                "status": stats.status.value,
                "subnet": stats.subnet,
                "total_successes": stats.total_successes,
                "total_bans": stats.total_bans,
                "total_conn_errors": stats.total_conn_errors,
                "consecutive_bans": stats.consecutive_bans,
                "consecutive_conn_errors": stats.consecutive_conn_errors,
            }
        return summary

    def log_summary(self) -> None:
        """Логирует финальную сводку здоровья пула прокси.

        Включает статистику по подсетям: сколько прокси в каждой,
        сколько мёртвых, были ли превентивные исключения.

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
            subnets=len(self._subnet_servers),
        )

        # Статистика по каждому прокси
        for server, stats in self._stats.items():
            logger.info(
                "proxy_stats",
                server=server,
                subnet=stats.subnet,
                status=stats.status.value,
                successes=stats.total_successes,
                bans=stats.total_bans,
                conn_errors=stats.total_conn_errors,
                consecutive_bans=stats.consecutive_bans,
            )

        # Статистика по подсетям
        for subnet, servers in self._subnet_servers.items():
            subnet_dead = sum(
                1 for srv in servers
                if self._stats[srv].status == ProxyStatus.DEAD
            )
            subnet_alive = len(servers) - subnet_dead
            subnet_total_bans = sum(
                self._stats[srv].total_bans for srv in servers
            )

            if subnet_dead > 0 or subnet_total_bans > 0:
                logger.info(
                    "subnet_stats",
                    subnet=f"{subnet}.0/24",
                    total=len(servers),
                    alive=subnet_alive,
                    dead=subnet_dead,
                    total_bans=subnet_total_bans,
                )
