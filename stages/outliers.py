import pandas as pd
from core.chain import PipelineNode

class IQRFilter(PipelineNode):
    """
    Класс для фильтрации выбросов с использованием метода IQR.
    
    Обрезает значения указанного столбца в пределах [Q1 - 1.5*IQR, Q3 + 1.5*IQR].
    """

    def __init__(self, column: str):
        """
        Инициализировать объект IQRFilter.
        
        Аргументы:
          column : str - название столбца для обработки выбросов.
        """

        super().__init__()
        self.column = column

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Обрезать выбросы в указанном столбце с использованием метода IQR.
        
        Аргументы:
          df : pd.DataFrame - исходный DataFrame.
            
        Возвращает:
          pd.DataFrame - DataFrame с обработанными выбросами.
        """

        q1, q3 = df[self.column].quantile([0.25, 0.75])
        iqr = q3 - q1
        low, high = q1 - 1.5 * iqr, q3 + 1.5 * iqr
        df[self.column] = df[self.column].clip(low, high)
        return df
