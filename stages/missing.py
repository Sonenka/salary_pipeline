import pandas as pd
from core.chain import PipelineNode

class MissingHandler(PipelineNode):
    """
    Класс для обработки пропущенных значений в данных.
    
    В числовых столбцах заполняет медианой, в категориальных - значением "unknown".
    """

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Обработать пропущенные значения в DataFrame.
        
        Аргументы:
            df : pd.DataFrame - DataFrame с возможными пропущенными значениями.
            
        Возвращает:
            pd.DataFrame - DataFrame с заполненными пропущенными значениями.
        """

        for col in df.select_dtypes(include="number"):
            df.loc[:, col] = df[col].fillna(df[col].median())

        for col in df.select_dtypes(include="object"):
            df.loc[:, col] = df[col].fillna("unknown")

        return df
