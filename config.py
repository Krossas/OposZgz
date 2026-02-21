import os
from pathlib import Path

# Directorio base del proyecto
BASE_DIR = Path(__file__).resolve().parent

# Base de datos
DB_PATH = BASE_DIR / "convocatorias.db"

# URL base
ZARAGOZA_URL = "https://www.zaragoza.es/oferta/"

# Niveles a filtrar
NIVELES_INTERES = ["C1", "C2"]

# Formatos de fecha posibles
FORMATO_FECHAS = [
    "%d/%m/%Y",
    "%d-%m-%Y",
    "%d.%m.%Y",
    "%Y-%m-%d",
]

# Timeout para requests (segundos)
REQUEST_TIMEOUT = 10

# Búsquedas de palabras clave para identificar nivel
KEYWORDS_C1 = ["C1", "Grupo C1"]
KEYWORDS_C2 = ["C2", "Grupo C2"]

# Archivo de salida CSV
CSV_OUTPUT = BASE_DIR / "convocatorias_estado.csv"
