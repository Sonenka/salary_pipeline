import re
from typing import Optional
import pandas as pd
from core.chain import PipelineNode
from config import ROLE_KEYWORDS

class SimpleEncoder(PipelineNode):
    """Класс для преобразования и кодирования категориальных признаков в числовые c one-hot encoding."""

    def __init__(self):
        """Инициализировать объект SimpleEncoder."""
        super().__init__()
        self.verbose = False

    def parse_gender(self, text: str | None) -> Optional[int]:
        """
        Извлечь пола из текстовой строки.
        
        Аргументы:
            text: object - текстовая строка с информацией о поле.
            
        Возвращает:
            Optional[int] - 1 для мужского пола, 0 для женского, None если пол не удалось определить.
        """

        t = text.lower()
        if "муж" in t or "male" in t:
            return 1
        if "жен" in t or "female" in t:
            return 0
        return None

    def parse_age(self, text: object | None) -> Optional[float]:
        """
        Извлечь возраст из текстовой строки.
        
        Аргументы: 
            text: object - текстовая строка с информацией о возрасте.
            
        Возвращает: 
            Optional[float] - возраст в годах или None, если возраст не удалось определить.
        """

        m = re.search(r"(\d{1,3})(?:[.,]\d+)?\s*(?:лет|год|года|years?)", text.lower())
        if m:
            return float(m.group(1))
        return None
    
    def detect_role_group(self, title: object | None) -> str:
        """
        Определить группу должности на основе ключевых слов в названии.
        
        Аргументы:
            title: object - название должности.
            
        Возвращает:
            str - группа должности (dev, sys, mgr, analyst, support, marketing, engineer, other).
        """

        if not isinstance(title, str):
            return "other"
        t = title.lower()
        for bucket, kws in ROLE_KEYWORDS.items():
            if any(k in t for k in kws):
                return bucket
        return "other"
    
    def parse_experience_years(self, text: object | None) -> Optional[float]:
        """
        Извлечь опыт работы из текстовой строки.
        
        Аргументы:
            text : object - текстовая строка с информацией об опыте работы.
            
        Возвращает:
            Optional[float] - опыт работы в годах (включая месяцы как десятичную часть).
        """
        
        text_lower = text.lower()
        
        pattern = r'(?:опыт работы\s*)?(\d+)\s*(?:лет|год|года|г\.|years?)(?:\s+(\d+)\s*(?:месяц|месяца|месяцев|мес\.|months?))?'
        
        experience_match = re.search(pattern, text_lower)
        
        if experience_match:
            yrs = int(experience_match.group(1)) if experience_match.group(1) else 0
            
            months = int(experience_match.group(2)) if experience_match.group(2) else 0
            
            return yrs + months / 12.0
        
        month_pattern = r'(\d+)\s*(?:месяц|месяца|месяцев|мес\.|months?)'
        month_match = re.search(month_pattern, text_lower)
        
        if month_match:
            months = int(month_match.group(1))
            return months / 12.0
        
        return None

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Преобразовать данные.
        
        Аргументы:
            df : pd.DataFrame - исходный DataFrame с сырыми данными.
            
        Возвращает:
        pd.DataFrame - DataFrame с преобразованными и закодированными признаками.
        """

        df = df.copy()

        if self.verbose:
            print("Начало кодирования признаков...")

        df["gender"] = df["Пол, возраст"].map(self.parse_gender).fillna(-1).astype(float)
        df["age"] = df["Пол, возраст"].map(self.parse_age)
        df["age"] = df["age"].clip(lower=18, upper=75)
        df["age"] = df["age"].fillna(df["age"].median())
        df.drop(columns=["Пол, возраст"], inplace=True)

        sex_dummies = pd.get_dummies(df["gender"], prefix="gender")
        df = pd.concat([df, sex_dummies], axis=1)

        split_cols = df["Город"].str.split(",", expand=True)
        df["City"] = split_cols.iloc[:, 0].str.strip()

        df["City"] = df["City"].replace(
            {
                "Saint Petersburg": "SPB",
                "Санкт-Петербург": "SPB",
                "Moscow": "MSK",
                "Москва": "MSK",
            }
        )

        df.loc[~df["City"].isin({"MSK", "SPB"}), "City"] = "Other"
        city_dummies = pd.get_dummies(df["City"], prefix="City")
        df = pd.concat([df, city_dummies], axis=1)
        df = df.drop(columns=["Город", "City"])

        df["Занятость"] = (
            df["Занятость"]
            .str.strip()
            .replace(
                {
                    "full time": "полная занятость",
                    "part time": "частичная занятость",
                    "volunteering": "волонтерство",
                    "work placement": "стажировка",
                    "project work": "проектная работа",
                }
            )
        )
        employ_dummies = pd.get_dummies(df["Занятость"])
        df = pd.concat([df, employ_dummies], axis=1)
        df = df.drop(columns=["Занятость"])

        df["График"] = (
            df["График"]
            .str.strip()
            .replace(
                {
                    "rotation based work": "вахтовый метод",
                    "flexible schedule": "гибкий график",
                    "shift schedule": "сменный график",
                    "full day": "полный день",
                    "remote working": "удаленная работа",
                }
            )
        )
        schedule_dummies = pd.get_dummies(df["График"])
        df = pd.concat([df, schedule_dummies], axis=1)
        df = df.drop(columns=["График"])

        df["role_group"] = df["Ищет работу на должность:"].map(self.detect_role_group)
        df = pd.get_dummies(df, columns=["role_group"], drop_first=True)
        df.drop(columns=["Ищет работу на должность:"], inplace=True)
        
        df["years_exp"] = df["Опыт (двойное нажатие для полной версии)"].map(self.parse_experience_years)
        med = df["years_exp"].median()
        df["years_exp"] = df["years_exp"].fillna(med).clip(lower=0, upper=45)
        df.drop(columns=["Опыт (двойное нажатие для полной версии)"], inplace=True)

        df["Авто"] = df["Авто"].replace(
            {
                "Не указано": "Unknown",
                "Имеется собственный автомобиль": "HasAuto",
            }
        )

        auto_dummies = pd.get_dummies(df["Авто"], prefix="Auto")
        df = pd.concat([df, auto_dummies], axis=1)
        df.drop(columns=["Авто"], inplace=True)

        df.drop(
            columns=[
                "Последенее/нынешнее место работы",
                "Последеняя/нынешняя должность",
                "Образование и ВУЗ",
                "Обновление резюме",
            ],
            errors="ignore",
            inplace=True,
        )

        if self.verbose:
            print("Кодирование завершено\n")
            print(f"Размер данных: {df.shape[0]} строк, {df.shape[1]} столбцов")
            print("\nПредпросмотр данных:")
            print(df.head())
            print("\nОсновные статистики:")
            print(f"Возраст: {df['age'].mean():.1f} ± {df['age'].std():.1f} лет")
            print(f"Опыт работы: {df['years_exp'].mean():.1f} ± {df['years_exp'].std():.1f} лет")
            print(f"Количество признаков: {len(df.columns)}\n")
        return df
