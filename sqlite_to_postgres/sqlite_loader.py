import sqlite3
from typing import List, Dict


class SQLiteLoader:
    def __init__(self, connection: sqlite3.Connection):
        self.connection = connection
        self.cursor = self.connection.cursor()

    def load_data(self) -> Dict[str, List]:
        """Загрузка данных из SQLite и их трансформация"""
        data = {}
        tables = ["actors", "movies", "writers", "movie_actors"]

        for table in tables:
            self.cursor.execute(f"SELECT * FROM {table};")
            data[table] = self.cursor.fetchall()

        return data
