import ftp_downloader
import file_converter
import db_loader

if __name__ == "__main__":
    try:
        print("📥 Начинаем скачивание файлов с FTP...")
        ftp_downloader.download_ftp_files()
        print("✅ Скачивание завершено!\n")

        print("🔄 Начинаем обработку файлов (MIS → CSV)...")
        file_converter.process_all_files()
        print("✅ Конвертация завершена!\n")

        print("📤 Загружаем данные в базу данных PostgreSQL...")
        db_loader.load_data_to_db()  # ✅ Загружаем данные без создания таблицы
        print("✅ Данные успешно загружены в PostgreSQL!\n")

        print("🎉 Процесс завершен!")

    except Exception as e:
        print(f"❌ Ошибка: {e}")
