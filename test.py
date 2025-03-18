import psycopg2

try:
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="postgres",  # Замени на свою БД, если есть
        user="postgres",
        password="lion2266"  # Укажи свой пароль
    )
    print("✅ Успешное подключение к PostgreSQL!")
    conn.close()
except Exception as e:
    print(f"❌ Ошибка подключения: {e}")
