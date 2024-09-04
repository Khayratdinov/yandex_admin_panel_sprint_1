import psycopg2
from psycopg2 import sql


def list_tables_in_schema(cursor, schema_name):
    cursor.execute(
        """
        SELECT tablename 
        FROM pg_tables 
        WHERE schemaname = %s;
        """,
        (schema_name,),
    )
    return [table[0] for table in cursor.fetchall()]


def delete_all_data_in_tables(connection, schema_name):
    with connection.cursor() as cursor:
        # Вывод информации о подключении
        cursor.execute("SELECT current_database(), current_user;")
        db_info = cursor.fetchone()
        print(f"Подключен к базе данных: {db_info[0]}, пользователь: {db_info[1]}")

        # Отключаем триггеры
        cursor.execute("SET session_replication_role = replica;")

        # Получаем список таблиц
        tables = list_tables_in_schema(cursor, schema_name)

        if not tables:
            print(f"В схеме '{schema_name}' нет таблиц для очистки.")
            return

        # Генерируем и выполняем команды DELETE для каждой таблицы с указанием схемы
        for table in tables:
            table_name_with_schema = sql.Identifier(schema_name, table)
            print(f"Удаление всех данных из таблицы: {schema_name}.{table}")
            cursor.execute(
                sql.SQL("DELETE FROM {} WHERE TRUE;").format(table_name_with_schema)
            )

        # Включаем триггеры обратно
        cursor.execute("SET session_replication_role = DEFAULT;")

    # Фиксируем изменения в базе данных
    connection.commit()


def main():
    # Параметры подключения к базе данных
    db_params = {
        "dbname": "sql-to-postgres-db",
        "user": "postgres",
        "password": "admin12345",
        "host": "127.0.0.1",  # или адрес вашего сервера
        "port": "5432",  # стандартный порт для PostgreSQL
    }

    # Подключаемся к базе данных PostgreSQL
    try:
        connection = psycopg2.connect(**db_params)
        print("Подключение к базе данных успешно.")

        # Указываем имя схемы, из которой нужно удалить данные
        schema_name = "content"

        # Удаляем все данные из таблиц в указанной схеме
        delete_all_data_in_tables(connection, schema_name)
        print("Все данные из таблиц были удалены.")

    except Exception as e:
        print(f"Произошла ошибка: {e}")

    finally:
        if connection:
            connection.close()
            print("Подключение к базе данных закрыто.")


if __name__ == "__main__":
    main()
