import uuid
from psycopg2.extras import execute_values
from dataclasses import dataclass
from typing import List, Dict


@dataclass(frozen=True)
class Genre:
    id: str
    name: str


@dataclass(frozen=True)
class Person:
    id: str
    full_name: str


@dataclass(frozen=True)
class FilmWork:
    id: str
    title: str
    description: str
    rating: float


@dataclass(frozen=True)
class FilmWorkGenre:
    id: str
    film_work_id: str
    genre_id: str


@dataclass(frozen=True)
class FilmWorkPerson:
    id: str
    film_work_id: str
    person_id: str
    role: str


class PostgresSaver:
    def __init__(self, pg_conn):
        self.pg_conn = pg_conn
        self.cursor = self.pg_conn.cursor()

    def save_all_data(self, data: Dict[str, List]):
        """Сохранение всех данных в PostgreSQL"""
        transformed_data = self._transform_data(data)
        self._save_genres(transformed_data["genres"])
        self._save_persons(transformed_data["persons"])
        self._save_film_works(transformed_data["film_works"])
        self._save_film_work_genres(transformed_data["film_work_genres"])
        self._save_film_work_persons(transformed_data["film_work_persons"])
        self.pg_conn.commit()

    def _transform_data(self, sqlite_data: Dict[str, List]) -> Dict[str, List]:
        """Трансформация данных из структуры SQLite в структуру PostgreSQL"""
        genres, persons, film_works = set(), set(), []
        film_work_genres, film_work_persons = [], []
        genre_map = {}

        for movie in sqlite_data["movies"]:
            movie_uuid = str(uuid.uuid4())
            title, genre_text, director, writer, plot, imdb_rating = (
                movie[4],
                movie[1],
                movie[2],
                movie[3],
                movie[5],
                movie[7],
            )

            imdb_rating = (
                float(imdb_rating)
                if imdb_rating.replace(".", "", 1).isdigit()
                else None
            )
            film_works.append(
                FilmWork(
                    id=movie_uuid, title=title, description=plot, rating=imdb_rating
                )
            )

            self._process_genres(
                genre_text, movie_uuid, genres, film_work_genres, genre_map
            )
            self._process_person(
                writer, movie_uuid, "writer", persons, film_work_persons
            )

        actor_dict = {actor[0]: actor[1] for actor in sqlite_data["actors"]}
        self._process_movie_actors(
            sqlite_data["movie_actors"],
            actor_dict,
            film_works,
            persons,
            film_work_persons,
        )

        return {
            "genres": list(genres),
            "persons": list(persons),
            "film_works": film_works,
            "film_work_genres": film_work_genres,
            "film_work_persons": film_work_persons,
        }

    def _process_genres(
        self, genre_text, movie_uuid, genres, film_work_genres, genre_map
    ):
        if genre_text:
            for g in genre_text.split(","):
                genre_name = g.strip()
                if genre_name not in genre_map:
                    genre_id = str(uuid.uuid4())
                    genre_map[genre_name] = genre_id
                    genres.add(Genre(id=genre_id, name=genre_name))

                film_work_genres.append(
                    FilmWorkGenre(
                        id=str(uuid.uuid4()),
                        film_work_id=movie_uuid,
                        genre_id=genre_map[genre_name],
                    )
                )

    def _process_person(self, name, movie_uuid, role, persons, film_work_persons):
        if name:
            person_id = str(uuid.uuid4())
            person = Person(id=person_id, full_name=name.strip())
            persons.add(person)
            film_work_persons.append(
                FilmWorkPerson(
                    id=str(uuid.uuid4()),
                    film_work_id=movie_uuid,
                    person_id=person.id,
                    role=role,
                )
            )

    def _process_movie_actors(
        self, movie_actors, actor_dict, film_works, persons, film_work_persons
    ):
        for movie_actor in movie_actors:
            movie_id, actor_id = movie_actor[0], movie_actor[1]
            movie_uuid = next((f.id for f in film_works if f.title == movie_id), None)
            actor_name = actor_dict.get(actor_id)

            if movie_uuid and actor_name:
                actor_uuid = str(uuid.uuid4())
                person = Person(id=actor_uuid, full_name=actor_name)
                persons.add(person)
                film_work_persons.append(
                    FilmWorkPerson(
                        id=str(uuid.uuid4()),
                        film_work_id=movie_uuid,
                        person_id=person.id,
                        role="actor",
                    )
                )

    def _get_all_genres(self):
        """Получение всех жанров из таблицы genre"""
        self.cursor.execute("SELECT id, name FROM content.genre;")
        return [Genre(id=row[0], name=row[1]) for row in self.cursor.fetchall()]

    def _save_genres(self, genres: List[Genre]):
        execute_values(
            self.cursor,
            "INSERT INTO content.genre (id, name) VALUES %s ON CONFLICT DO NOTHING;",
            [(g.id, g.name) for g in genres],
        )

    def _save_persons(self, persons: List[Person]):
        execute_values(
            self.cursor,
            "INSERT INTO content.person (id, full_name) VALUES %s ON CONFLICT DO NOTHING;",
            [(p.id, p.full_name) for p in persons],
        )

    def _save_film_works(self, film_works: List[FilmWork]):
        film_work_data = [
            (
                f.id,
                f.title,
                f.description,
                f.rating,
                "movie",
                None,
                None,
                None,
                None,
                None,
            )
            for f in film_works
        ]
        execute_values(
            self.cursor,
            "INSERT INTO content.film_work (id, title, description, rating, type, creation_date, certificate, file_path, created_at, updated_at) VALUES %s ON CONFLICT (id) DO NOTHING;",
            film_work_data,
        )

    def _save_film_work_genres(self, film_work_genres: List[FilmWorkGenre]):
        genre_ids = {genre.id for genre in self._get_all_genres()}
        valid_film_work_genres = [
            (f.id, f.film_work_id, f.genre_id)
            for f in film_work_genres
            if f.genre_id in genre_ids
        ]

        if valid_film_work_genres:
            execute_values(
                self.cursor,
                "INSERT INTO content.genre_film_work (id, film_work_id, genre_id) VALUES %s ON CONFLICT DO NOTHING;",
                valid_film_work_genres,
            )

    def _save_film_work_persons(self, film_work_persons: List[FilmWorkPerson]):
        execute_values(
            self.cursor,
            "INSERT INTO content.person_film_work (id, film_work_id, person_id, role) VALUES %s ON CONFLICT DO NOTHING;",
            [(f.id, f.film_work_id, f.person_id, f.role) for f in film_work_persons],
        )
