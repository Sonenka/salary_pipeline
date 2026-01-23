from pathlib import Path
import pandas as pd
from core.chain import PipelineNode

class CSVLoader(PipelineNode):
    '''Класс для загрузки данных из CSV файла.'''

    def __init__(self, path: Path) -> None:
        '''
        Инициализирует объект CSVLoader.
        
        Аргументы:
          path : Path - путь к CSV файлу.
        '''

        super().__init__()
        self.path = path

    def process(self, frame):
        '''
        Загружает данные из CSV файла.
        
        Аргументы:
          frame : любое - игнорируется, требуется для совместимости с PipelineNode.
            
        Возвращает:
          pd.DataFrame - загруженные данные из CSV файла.
            
        Исключения:
          FileNotFoundError - если указанный файл не существует.
        '''

        if not self.path.exists():
            raise FileNotFoundError(self.path)
        else:
            df = pd.read_csv(
                self.path,
                engine="python",
                index_col=0,
            )
            df.columns = [c.strip() for c in df.columns]
            return df

