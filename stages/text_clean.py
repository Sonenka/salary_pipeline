import re
import pandas as pd
from core.chain import PipelineNode

class TextNormalizer(PipelineNode):
    """Класс для нормализации текстовых данных."""

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Очистить текстовые данные в DataFrame.
        
        Аргументы:
            df : pd.DataFrame - DataFrame с текстовыми данными.
            
        Возвращает:
            pd.DataFrame - DataFrame с очищенными текстовыми данными.
        """
        
        def clean(val):
            """
            Вспомогательная функция для очистки текста.
            
            Аргументы:
                val : str - исходная текстовая строка.
                
            Возвращает:
                str - очищенная текстовая строка.
            """
            if not isinstance(val, str):
                return val
            val = val.replace("\ufeff", "").replace("\xa0", " ")
            val = re.sub(r"[\r\n\t]+", " ", val)
            return re.sub(r"\s+", " ", val).strip()

        for col in df.select_dtypes(include="object"):
            df[col] = df[col].map(clean)

        return df
