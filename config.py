import os

# FTP конфигурация
FTP_SERVER = os.getenv("FTP_SERVER", "178.216.209.185")
FTP_PORT = os.getenv("FTP_PORT", "47855")  # Оставляем как строку, преобразуем в int в ftp_downloader.py
FTP_USER = os.getenv("FTP_USER", "Hydras")
FTP_PASS = os.getenv("FTP_PASS", "Ott_user")
FTP_DIR = os.getenv("FTP_DIR", "/path_1")

# PostgreSQL конфигурация
POSTGRESQL = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "database": os.getenv("POSTGRES_DB", "weather_data"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "lion2266")
}

# Локальные директории
DATA_DIR = "data"
RAW_MIS_DIR = f"{DATA_DIR}/raw_mis"
PROCESSED_CSV_DIR = f"{DATA_DIR}/processed_csv"