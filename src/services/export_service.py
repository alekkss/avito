"""Сервис экспорта данных в Excel.

Формирует Excel-файл с одним листом, содержащим все
нормализованные товары. Таблица оптимизирована для
фильтрации и сортировки в Excel: автофильтры,
фиксированная шапка, автоширина столбцов.
"""

from pathlib import Path

from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.worksheet import Worksheet

from src.config import ExportSettings, get_logger
from src.models import NormalizedProduct
from src.repositories.base import BaseProductRepository

logger = get_logger("export_service")


# Определение столбцов отчёта: (заголовок, ширина в символах)
REPORT_COLUMNS: list[tuple[str, int]] = [
    ("Нормализованное название", 40),
    ("Категория", 20),
    ("Характеристики", 35),
    ("Цена (руб.)", 14),
    ("Оригинальное название", 45),
    ("Продавец", 20),
    ("Рейтинг продавца", 16),
    ("Отзывы", 16),
    ("Ссылка", 60),
    ("Описание", 50),
    ("Дата парсинга", 20),
]


class ExportService:
    """Сервис для экспорта товаров в Excel-файл.

    Читает нормализованные товары из репозитория и формирует
    отформатированный Excel-файл с автофильтрами и стилями
    для удобной работы в Excel.

    Attributes:
        _repository: Репозиторий для чтения нормализованных товаров.
        _settings: Настройки экспорта (путь к файлу).
    """

    def __init__(
        self,
        repository: BaseProductRepository,
        settings: ExportSettings,
    ) -> None:
        """Инициализирует сервис экспорта.

        Args:
            repository: Репозиторий с нормализованными товарами.
            settings: Настройки экспорта (путь к выходному файлу).
        """
        self._repository = repository
        self._settings = settings

    def export(self) -> str:
        """Экспортирует все нормализованные товары в Excel-файл.

        Основной публичный метод. Читает данные из репозитория,
        создаёт Excel-файл с форматированной таблицей и сохраняет
        по указанному пути.

        Returns:
            Абсолютный путь к созданному Excel-файлу.
        """
        products = self._repository.get_all_normalized_products()

        if not products:
            logger.warning("no_products_to_export")
            return ""

        logger.info(
            "export_started",
            products_count=len(products),
            export_path=self._settings.export_path,
        )

        wb = Workbook()
        ws = wb.active
        if ws is None:
            ws = wb.create_sheet()
        ws.title = "Товары Avito"

        self._write_header(ws)
        self._write_data(ws, products)
        self._apply_formatting(ws, len(products))

        output_path = self._save_workbook(wb)

        unique_titles = len({p.normalized_title for p in products})
        logger.info(
            "export_completed",
            products_count=len(products),
            unique_products=unique_titles,
            export_path=output_path,
        )

        return output_path

    def _write_header(self, ws: Worksheet) -> None:
        """Записывает строку заголовков в первую строку листа.

        Args:
            ws: Рабочий лист Excel.
        """
        header_font = Font(
            name="Calibri",
            bold=True,
            size=11,
            color="FFFFFF",
        )
        header_fill = PatternFill(
            start_color="4472C4",
            end_color="4472C4",
            fill_type="solid",
        )
        header_alignment = Alignment(
            horizontal="center",
            vertical="center",
            wrap_text=True,
        )
        thin_border = Border(
            left=Side(style="thin"),
            right=Side(style="thin"),
            top=Side(style="thin"),
            bottom=Side(style="thin"),
        )

        for col_index, (title, _) in enumerate(REPORT_COLUMNS, start=1):
            cell = ws.cell(row=1, column=col_index, value=title)
            cell.font = header_font
            cell.fill = header_fill
            cell.alignment = header_alignment
            cell.border = thin_border

    def _write_data(
        self,
        ws: Worksheet,
        products: list[NormalizedProduct],
    ) -> None:
        """Записывает данные товаров в лист начиная со второй строки.

        Args:
            ws: Рабочий лист Excel.
            products: Список нормализованных товаров.
        """
        data_alignment = Alignment(
            vertical="top",
            wrap_text=False,
        )
        price_alignment = Alignment(
            horizontal="right",
            vertical="top",
        )
        thin_border = Border(
            left=Side(style="thin"),
            right=Side(style="thin"),
            top=Side(style="thin"),
            bottom=Side(style="thin"),
        )
        link_font = Font(
            name="Calibri",
            size=10,
            color="0563C1",
            underline="single",
        )
        data_font = Font(
            name="Calibri",
            size=10,
        )

        for row_index, product in enumerate(products, start=2):
            row_data = self._product_to_row(product)

            for col_index, value in enumerate(row_data, start=1):
                cell = ws.cell(
                    row=row_index,
                    column=col_index,
                    value=value,
                )
                cell.font = data_font
                cell.alignment = data_alignment
                cell.border = thin_border

            # Форматирование столбца "Цена"
            price_cell = ws.cell(row=row_index, column=4)
            price_cell.alignment = price_alignment
            price_cell.number_format = "#,##0"

            # Форматирование столбца "Ссылка" как гиперссылка
            link_cell = ws.cell(row=row_index, column=9)
            url = product.full_url
            if url:
                link_cell.hyperlink = url
                link_cell.font = link_font

            # Чередующийся цвет строк для читаемости
            if row_index % 2 == 0:
                even_fill = PatternFill(
                    start_color="D9E2F3",
                    end_color="D9E2F3",
                    fill_type="solid",
                )
                for col_index in range(1, len(REPORT_COLUMNS) + 1):
                    ws.cell(row=row_index, column=col_index).fill = even_fill

    def _product_to_row(
        self, product: NormalizedProduct
    ) -> list[str | int]:
        """Преобразует NormalizedProduct в список значений строки.

        Порядок значений соответствует порядку столбцов
        в REPORT_COLUMNS.

        Args:
            product: Нормализованный товар.

        Returns:
            Список значений для одной строки Excel.
        """
        scraped_date = product.scraped_at.strftime("%Y-%m-%d %H:%M")

        return [
            product.normalized_title,
            product.product_category,
            product.key_specs,
            product.price,
            product.original_title,
            product.seller_name,
            product.seller_rating,
            product.seller_reviews,
            product.full_url,
            product.description[:200],
            scraped_date,
        ]

    def _apply_formatting(
        self, ws: Worksheet, data_rows: int
    ) -> None:
        """Применяет финальное форматирование к листу.

        Устанавливает ширину столбцов, автофильтры и фиксирует
        первую строку (шапку) для удобной прокрутки.

        Args:
            ws: Рабочий лист Excel.
            data_rows: Количество строк данных (без заголовка).
        """
        # Ширина столбцов
        for col_index, (_, width) in enumerate(REPORT_COLUMNS, start=1):
            col_letter = get_column_letter(col_index)
            ws.column_dimensions[col_letter].width = width

        # Автофильтр на все столбцы
        last_col_letter = get_column_letter(len(REPORT_COLUMNS))
        last_row = data_rows + 1
        ws.auto_filter.ref = f"A1:{last_col_letter}{last_row}"

        # Фиксация первой строки (заголовок)
        ws.freeze_panes = "A2"

        # Высота строки заголовка
        ws.row_dimensions[1].height = 30

        logger.debug(
            "formatting_applied",
            columns=len(REPORT_COLUMNS),
            data_rows=data_rows,
        )

    def _save_workbook(self, wb: Workbook) -> str:
        """Сохраняет Excel-файл на диск.

        Создаёт директорию для файла если она не существует.

        Args:
            wb: Объект Workbook для сохранения.

        Returns:
            Абсолютный путь к сохранённому файлу.
        """
        output_path = Path(self._settings.export_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        wb.save(str(output_path))
        absolute_path = str(output_path.resolve())

        logger.info(
            "workbook_saved",
            path=absolute_path,
        )

        return absolute_path
