import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from config import POSTGRESQL, PROCESSED_CSV_DIR
import os
from datetime import datetime
import pytz
import concurrent.futures
import time

PROCESSED_FILES_TRACKER = os.path.join(PROCESSED_CSV_DIR, "processed_files.txt")

def load_processed_files():
    if os.path.exists(PROCESSED_FILES_TRACKER):
        with open(PROCESSED_FILES_TRACKER, "r") as f:
            return set(f.read().splitlines())
    return set()

def save_processed_file(filename):
    with open(PROCESSED_FILES_TRACKER, "a") as f:
        f.write(f"{filename}\n")

def process_file(file_path, conn, almaty_tz, utc_tz):
    rows_to_insert = []
    rows_inserted = 0
    df = pd.read_csv(file_path)
    df.columns = df.columns.str.lower()
    required_columns = {"station", "sensor", "date", "time", "value"}
    if not required_columns.issubset(df.columns):
        print(f"⚠️ Пропущен файл {os.path.basename(file_path)} - отсутствуют необходимые колонки")
        return rows_to_insert, rows_inserted
    df["date"] = pd.to_datetime(df["date"].astype(str), format="%Y%m%d", errors="coerce").dt.date
    df["time"] = df["time"].astype(str).str.zfill(6)
    df["time"] = pd.to_datetime(df["time"], format="%H%M%S", errors="coerce").dt.time
    df = df.dropna(subset=["date", "time"])
    for _, row in df.iterrows():
        local_time = datetime.combine(row["date"], row["time"])
        timestamp_value = almaty_tz.localize(local_time).astimezone(utc_tz)
        rows_to_insert.append((
            str(row["station"]), str(row["sensor"]), row["date"], row["time"], row["value"], timestamp_value
        ))
        rows_inserted += 1
    return rows_to_insert, rows_inserted

def load_data_to_db():
    start_time = time.time()
    conn = psycopg2.connect(**POSTGRESQL)
    files_processed = 0
    total_rows_inserted = 0
    almaty_tz = pytz.timezone("Asia/Almaty")
    utc_tz = pytz.utc

    processed_files = load_processed_files()
    csv_files = [os.path.join(PROCESSED_CSV_DIR, f) for f in os.listdir(PROCESSED_CSV_DIR) if f.endswith(".csv") and f not in processed_files]
    if not csv_files:
        print("⚠️ Нет новых файлов для загрузки.")
        conn.close()
        return

    all_rows_to_insert = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        future_to_file = {executor.submit(process_file, file_path, conn, almaty_tz, utc_tz): file_path for file_path in csv_files}
        for future in concurrent.futures.as_completed(future_to_file):
            file_path = future_to_file[future]
            file_name = os.path.basename(file_path)
            try:
                rows_to_insert, rows_inserted = future.result()
                all_rows_to_insert.extend(rows_to_insert)
                total_rows_inserted += rows_inserted
                files_processed += 1
                if rows_inserted > 0:
                    print(f"✅ Обработан файл: {file_name} ({rows_inserted} строк добавлено)")
                else:
                    print(f"⏩ Пропущен файл: {file_name} (0 строк для вставки)")
                save_processed_file(file_name)  # Записываем всегда
            except Exception as e:
                print(f"❌ Ошибка при обработке {file_name}: {e}")

    if all_rows_to_insert:
        cursor = conn.cursor()
        execute_batch(cursor, """
            INSERT INTO weather_data (station, sensor, date, time, value, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (station, sensor, date, time) DO NOTHING;
        """, all_rows_to_insert, page_size=1000)
        conn.commit()
        cursor.close()
        print(f"📤 Загружено {total_rows_inserted} строк в БД.")

    conn.close()
    elapsed_time = time.time() - start_time
    print(f"🎉 Загрузка завершена! Файлов: {files_processed}. Время: {elapsed_time:.2f} сек")

if __name__ == "__main__":
    load_data_to_db()