from ftplib import FTP
import os
import re
from config import FTP_SERVER, FTP_PORT, FTP_USER, FTP_PASS, FTP_DIR, RAW_MIS_DIR

LAST_FILE_TRACKER = os.path.join(RAW_MIS_DIR, "last_file.txt")  # Файл для отслеживания последнего скачанного файла

def get_last_downloaded_file():
    """Читает имя последнего скачанного файла из last_file.txt"""
    if os.path.exists(LAST_FILE_TRACKER):
        with open(LAST_FILE_TRACKER, "r") as f:
            return f.read().strip()
    return None

def save_last_downloaded_file(filename):
    """Сохраняет имя последнего скачанного файла"""
    with open(LAST_FILE_TRACKER, "w") as f:
        f.write(filename)

def extract_timestamp(filename):
    """Извлекает дату и время из имени файла """
    match = re.search(r"_(\d{14})\.MIS$", filename)
    return match.group(1) if match else "00000000000000"  # Если не найдено, ставим минимальное значение

def download_ftp_files():
    os.makedirs(RAW_MIS_DIR, exist_ok=True)

    ftp = FTP()
    ftp.connect(FTP_SERVER, int(FTP_PORT))
    ftp.login(FTP_USER, FTP_PASS)
    ftp.cwd(FTP_DIR)

    # Получаем список файлов и сортируем их по дате из имени файла
    filenames = ftp.nlst()
    filenames = sorted(filenames, key=extract_timestamp)  # Теперь файлы загружаются по порядку!

    last_file = get_last_downloaded_file()

    # Если уже есть скачанные файлы, оставляем только новые
    if last_file and last_file in filenames:
        filenames = filenames[filenames.index(last_file) + 1:]  # Оставляем только новые файлы

    if not filenames:
        print("✅ Нет новых файлов для скачивания.")
        ftp.quit()
        return

    for filename in filenames:
        local_path = os.path.join(RAW_MIS_DIR, filename)

        # Если файл уже скачан, пропускаем
        if os.path.exists(local_path):
            print(f"⚠️ Файл {filename} уже скачан ранее, пропускаем.")
            continue

        # Скачиваем файл
        with open(local_path, "wb") as file:
            ftp.retrbinary(f"RETR {filename}", file.write)
        print(f"✅ Скачан: {filename}")

        # Обновляем последний скачанный файл
        save_last_downloaded_file(filename)

    ftp.quit()

if __name__ == "__main__":
    download_ftp_files()
