import os
import re
import csv
from config import RAW_MIS_DIR, PROCESSED_CSV_DIR

def convert_mis_to_csv(mis_filename):
    raw_path = os.path.join(RAW_MIS_DIR, mis_filename)
    csv_filename = mis_filename.replace(".MIS", ".csv").replace(".mis", ".csv")
    csv_path = os.path.join(PROCESSED_CSV_DIR, csv_filename)

    # Проверяем, существует ли CSV и актуален ли он
    if os.path.exists(csv_path):
        mis_mtime = os.path.getmtime(raw_path)
        csv_mtime = os.path.getmtime(csv_path)
        if csv_mtime >= mis_mtime:
            return

    os.makedirs(PROCESSED_CSV_DIR, exist_ok=True)

    with open(raw_path, "r", encoding="utf-8") as f:
        content = f.read()

    content = content.replace("[", "").replace("]", "").replace("><", ">\n<")
    lines = content.splitlines()

    station, sensor, date_format = None, None, None
    parsed_rows = []

    for line in lines:
        line = line.strip()
        if "<STATION>" in line:
            station_match = re.search(r"<STATION>(.*?)</STATION>", line)
            if station_match:
                station = station_match.group(1)
        elif "<SENSOR>" in line:
            sensor_match = re.search(r"<SENSOR>(.*?)</SENSOR>", line)
            if sensor_match:
                sensor = sensor_match.group(1)
        elif "<DATEFORMAT>" in line:
            df_match = re.search(r"<DATEFORMAT>(.*?)</DATEFORMAT>", line)
            if df_match:
                date_format = df_match.group(1)
        else:
            parts = line.split(";")
            if len(parts) == 3:
                date_str = parts[0].strip()
                time_str = parts[1].strip()
                value_str = parts[2].strip()
                if station and sensor and date_str and time_str and value_str:
                    parsed_rows.append([station, sensor, date_str, time_str, value_str])

    if parsed_rows:
        with open(csv_path, "w", encoding="utf-8", newline="") as out:
            writer = csv.writer(out)
            writer.writerow(["STATION", "SENSOR", "DATE", "TIME", "VALUE"])
            writer.writerows(parsed_rows)
    else:
        print(f"⚠️ Нет данных для сохранения в {csv_filename}")

def process_all_files():
    for file_name in os.listdir(RAW_MIS_DIR):
        if file_name.lower().endswith(".mis"):
            convert_mis_to_csv(file_name)

if __name__ == "__main__":
    process_all_files()