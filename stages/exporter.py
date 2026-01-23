from pathlib import Path
import numpy as np
import pandas as pd
from core.chain import PipelineNode
from config import X_FILENAME, Y_FILENAME, TARGET_COLUMN

class NumpyWriter(PipelineNode):
    '''
    Класс для сохранения данных в формате NumPy.
    
    Сохраняет признаки (X) и целевую переменную (y) в отдельные файлы .npy.
    '''

    def __init__(self, source: Path):
        '''
        Инициализирует объект NumpyWriter.
        
        Аргументы:
          source : Path - путь к исходному CSV файлу.
        '''

        super().__init__()
        self.out_dir = source.parent

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        '''
        Разделяет данные на признаки и целевую переменную и сохраняет их в формате NumPy.
        
        Аргументы:
          df : pd.DataFrame - DataFrame.
            
        Возвращает:
          pd.DataFrame - DataFrame без изменений.
        '''

        X = df.drop(columns=[TARGET_COLUMN])
        y = df[TARGET_COLUMN]

        np.save(self.out_dir / X_FILENAME, X)
        np.save(self.out_dir / Y_FILENAME, y)

        return df
