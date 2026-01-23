import argparse
import logging
from pathlib import Path

from stages.reader import CSVLoader
from stages.text_clean import TextNormalizer
from stages.salary_norm import SalaryNormalizer
from stages.outliers import IQRFilter
from stages.missing import MissingHandler
from stages.features import SimpleEncoder
from stages.exporter import NumpyWriter
from config import TARGET_COLUMN

logging.basicConfig(level=logging.WARNING)

def build_pipeline(csv_path: Path, verbose: bool = False):
    '''
    Создает пайплайн обработки данных.
    
    Parameters
    ----------
    csv_path : Path
        Путь к исходному CSV файлу.
    verbose : bool, optional
        Если True, включает подробный вывод процесса обработки.
        
    Returns
    -------
    CSVLoader
        Корневой узел пайплайна.
    '''
    reader = CSVLoader(csv_path)
    cleaner = TextNormalizer()
    salary = SalaryNormalizer(TARGET_COLUMN)
    outliers = IQRFilter(TARGET_COLUMN)
    missing = MissingHandler()
    encoder = SimpleEncoder()
    writer = NumpyWriter(csv_path)

    encoder.verbose = verbose

    reader \
        .set_next(cleaner) \
        .set_next(salary) \
        .set_next(outliers) \
        .set_next(missing) \
        .set_next(encoder) \
        .set_next(writer)

    return reader

def main():
    '''
    Основная функция для запуска пайплайна обработки данных.
    
    Ожидает путь к CSV файлу в качестве аргумента командной строки.
    При использовании флага --verbose выводит подробную информацию о процессе.
    '''
    parser = argparse.ArgumentParser(
        description="Пайплайн обработки данных о зарплатах",
        epilog="Примеры использования:\n"
               "  py app.py hh.csv              # Тихий режим (по умолчанию)\n"
               "  py app.py hh.csv --verbose    # Подробный вывод процесса"
    )
    
    parser.add_argument("csv_path", type=Path, 
                       help="Путь к CSV файлу с данными")
    
    parser.add_argument("-v", "--verbose", action="store_true",
                       help="Включить подробный вывод процесса обработки")
    
    args = parser.parse_args()

    if args.verbose:
        print(f"Запуск обработки файла {args.csv_path}")

    pipeline = build_pipeline(args.csv_path, args.verbose)
    pipeline.handle(None)
    
    if args.verbose:
        print(f"Обработка завершена")

if __name__ == "__main__":
    main()