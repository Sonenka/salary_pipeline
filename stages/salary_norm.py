import pandas as pd
from core.chain import PipelineNode
from config import RATES

class SalaryNormalizer(PipelineNode):
    '''
    Класс для нормализации зарплат в рублях.
    
    Извлекает числовое значение и валюту из строки зарплаты,
    затем конвертирует все значения в рубли по установленным курсам.
    '''

    def __init__(self, column: str):
        '''
        Инициализирует объект SalaryNormalizer.
        
        Аргументы:
          column : str - название столбца с данными о зарплате.
        '''

        super().__init__()
        self.column = column

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        '''
        Нормализует зарплаты, приводя их к рублевому эквиваленту.
        
        Аргументы:
          df : pd.DataFrame - DataFrame с данными о зарплате.
            
        Возвращает:
        pd.DataFrame - DataFrame с нормализованными зарплатами в рублях.
        '''

        values = df[self.column].astype(str)

        numbers = values.str.extract(r"(\d+[.,]?\d*)")[0]
        numbers = numbers.str.replace(",", ".").astype(float)

        currency = values.str.extract(r"([A-Za-zА-Яа-я]+)")[0]
        currency = currency.str.lower().fillna("rub")

        df[self.column] = [
            val * RATES.get(cur, 1)
            for val, cur in zip(numbers, currency)
        ]

        return df
