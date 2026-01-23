from typing import Dict, List

TARGET_COLUMN = "ЗП"

X_FILENAME = "x_data.npy"
Y_FILENAME = "y_data.npy"

ROLE_KEYWORDS: Dict[str, List[str]] = {
    "dev": ["программист", "разработчик", "developer", "java", "python", "php", "frontend", "backend", "qa"],
    "sys": ["системн", "администратор", "devops", "dba", "сетев"],
    "mgr": ["менедж", "руководител", "начальник", "lead", "project", "product"],
    "analyst": ["аналитик", "data", "analysis", "bi"],
    "support": ["поддерж", "support", "helpdesk", "оператор"],
    "marketing": ["маркет", "seo", "контент", "дизайн"],
    "engineer": ["инженер", "техник", "электрик", "монтаж"],
}

RATES = {
    "AZN": 44.41,
    "BYN": 26.67,
    "EUR": 89.20,
    "KGS": 0.86,
    "KZT": 0.15,
    "RUB": 1.0,
    "UAH": 1.75,
    "USD": 75.50
}