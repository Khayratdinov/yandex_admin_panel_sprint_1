import sqlite3
import psycopg2
import yaml
import logging
from sqlite_loader import SQLiteLoader
from postgres_saver import PostgresSaver


def load_config():
    """Загрузка конфигурации из файла config.yaml"""
    with open("config.yaml", "r") as config_file:
        config = yaml.safe_load(config_file)
    return config


def setup_logging(config):
    """Настройка логирования"""
    logging.basicConfig(
        level=config["logging"]["level"],
        format=config["logging"]["format"],
        filename=config["logging"].get(
            "file"
        ),  # None если не указан файл, логи выводятся в консоль
    )


def load_from_sqlite(sqlite_conn: sqlite3.Connection, pg_conn):
    """Основной метод загрузки данных из SQLite в PostgreSQL"""
    sqlite_loader = SQLiteLoader(sqlite_conn)
    postgres_saver = PostgresSaver(pg_conn)

    data = sqlite_loader.load_data()
    postgres_saver.save_all_data(data)


if __name__ == "__main__":
    config = load_config()
    setup_logging(config)

    sqlite_db = config["sqlite"]["database"]
    postgres_config = config["postgres"]

    try:
        with sqlite3.connect(sqlite_db) as sqlite_conn, psycopg2.connect(
            **postgres_config
        ) as pg_conn:
            load_from_sqlite(sqlite_conn, pg_conn)
    except (sqlite3.Error, psycopg2.Error) as e:
        logging.error(f"Ошибка при подключении к базе данных: {e}")
    except Exception as e:
        logging.error(f"Неизвестная ошибка: {e}")
