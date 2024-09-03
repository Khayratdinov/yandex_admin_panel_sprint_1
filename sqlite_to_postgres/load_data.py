import sqlite3
import uuid
import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor, execute_values


class SQLiteLoader:
    def __init__(self, connection: sqlite3.Connection):
        self.connection = connection
        self.cursor = self.connection.cursor()

    def load_movies(self):
        """Load data from SQLite and transform it"""
        self.cursor.execute("SELECT * FROM actors;")
        actors_data = self.cursor.fetchall()

        self.cursor.execute("SELECT * FROM movies;")
        movies_data = self.cursor.fetchall()

        self.cursor.execute("SELECT * FROM writers;")
        writers_data = self.cursor.fetchall()

        self.cursor.execute("SELECT * FROM movie_actors;")
        movie_actors_data = self.cursor.fetchall()

        return {
            "actors": actors_data,
            "movies": movies_data,
            "writers": writers_data,
            "movie_actors": movie_actors_data,
        }


class PostgresSaver:
    def __init__(self, pg_conn: _connection):
        self.pg_conn = pg_conn
        self.cursor = self.pg_conn.cursor()

    def save_all_data(self, data):
        """Save the transformed data into PostgreSQL"""
        transformed_data = self.transform_data(data)

        # Save genres first
        self.save_genres(transformed_data["genre"])

        # Save persons before film works and relationships
        self.save_persons(transformed_data["person"])
        self.save_film_work(transformed_data["film_work"])

        # Finally, save the relationships
        self.save_film_work_genres(transformed_data["film_work_genre"])
        self.save_film_work_persons(transformed_data["film_work_person"])

        self.pg_conn.commit()

    def transform_data(self, sqlite_data):
        """Transform data from SQLite structure to PostgreSQL structure"""
        film_work = []
        genre = set()
        person = set()
        film_work_genre = []
        film_work_person = []

        genre_map = {}  # To store the mapping of genre names to UUIDs
        movie_id_map = {}

        # First, collect all genres and generate UUIDs for them
        for movie in sqlite_data["movies"]:
            title, genre_text = movie[4], movie[1]

            if genre_text:
                for g in genre_text.split(","):
                    genre_name = g.strip()
                    if genre_name not in genre_map:
                        genre_id = uuid.uuid4()
                        genre_map[genre_name] = str(genre_id)
                        genre.add((str(genre_id), genre_name))

        for movie in sqlite_data["movies"]:
            movie_uuid = uuid.uuid4()
            movie_id_map[movie[0]] = movie_uuid
            title, genre_text, director, writer, plot, ratings, imdb_rating, writers = (
                movie[4],
                movie[1],
                movie[2],
                movie[3],
                movie[5],
                movie[6],
                movie[7],
                movie[8],
            )

            # Handle 'N/A' or invalid ratings
            if imdb_rating == "N/A" or not imdb_rating.replace(".", "", 1).isdigit():
                imdb_rating = None  # or set to a default value like 0.0

            film_work.append(
                (str(movie_uuid), title, plot, imdb_rating)
            )  # Adjust as needed

            if genre_text:
                for g in genre_text.split(","):
                    genre_id = genre_map[g.strip()]
                    film_work_genre.append(
                        (str(uuid.uuid4()), str(movie_uuid), genre_id)
                    )

            if writers:
                for writer_name in writers.split(","):
                    writer_id = uuid.uuid4()
                    person.add((str(writer_id), writer_name.strip()))
                    film_work_person.append(
                        (str(uuid.uuid4()), str(movie_uuid), str(writer_id), "writer")
                    )

        actor_dict = {actor[0]: actor[1] for actor in sqlite_data["actors"]}

        for movie_actor in sqlite_data["movie_actors"]:
            movie_uuid = movie_id_map.get(movie_actor[0])
            actor_name = actor_dict.get(movie_actor[1])
            if movie_uuid and actor_name:
                actor_id = uuid.uuid4()
                person.add((str(actor_id), actor_name))
                film_work_person.append(
                    (str(uuid.uuid4()), str(movie_uuid), str(actor_id), "actor")
                )

        return {
            "film_work": film_work,
            "genre": list(genre),
            "person": list(person),
            "film_work_genre": film_work_genre,
            "film_work_person": film_work_person,
        }

    def save_genres(self, genres):
        genre_insert_query = (
            "INSERT INTO content.genre (id, name) VALUES %s ON CONFLICT DO NOTHING;"
        )
        execute_values(self.cursor, genre_insert_query, genres)

    def save_persons(self, persons):
        person_insert_query = "INSERT INTO content.person (id, full_name) VALUES %s ON CONFLICT DO NOTHING;"
        execute_values(self.cursor, person_insert_query, persons)

    def save_film_work(self, film_work):
        film_work_insert_query = """
        INSERT INTO content.film_work (
            id, title, description, rating, type, creation_date, certificate, file_path, created_at, updated_at
        )
        VALUES %s
        ON CONFLICT (id) DO NOTHING;
        """

        # Ensure UUIDs are converted to strings and provide values matching the columns in the INSERT statement
        film_work = [
            (str(id), title, description, rating, "movie", None, None, None, None, None)
            for id, title, description, rating in film_work
        ]

        execute_values(self.cursor, film_work_insert_query, film_work)

    def save_film_work_genres(self, film_work_genres):
        film_work_genre_insert_query = """
        INSERT INTO content.genre_film_work (id, film_work_id, genre_id)
        VALUES %s
        ON CONFLICT DO NOTHING;
        """
        execute_values(self.cursor, film_work_genre_insert_query, film_work_genres)

    def save_film_work_persons(self, film_work_persons):
        film_work_person_insert_query = """
        INSERT INTO content.person_film_work (id, film_work_id, person_id, role)
        VALUES %s
        ON CONFLICT DO NOTHING;
        """
        execute_values(self.cursor, film_work_person_insert_query, film_work_persons)


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    """Основной метод загрузки данных из SQLite в Postgres"""
    postgres_saver = PostgresSaver(pg_conn)
    sqlite_loader = SQLiteLoader(connection)

    data = sqlite_loader.load_movies()
    postgres_saver.save_all_data(data)


if __name__ == "__main__":
    dsl = {
        "dbname": "sql-to-postgres-db",
        "user": "postgres",
        "password": "admin12345",
        "host": "127.0.0.1",
        "port": 5432,
    }
    with sqlite3.connect("db.sqlite") as sqlite_conn, psycopg2.connect(
        **dsl, cursor_factory=DictCursor
    ) as pg_conn:
        load_from_sqlite(sqlite_conn, pg_conn)
