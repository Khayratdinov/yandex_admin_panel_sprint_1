-- Создание схемы для контента
CREATE SCHEMA IF NOT EXISTS content;


-- Таблица для жанров кинопроизведений
CREATE TABLE IF NOT EXISTS content.genre (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Таблица для кинопроизведений
CREATE TABLE IF NOT EXISTS content.film_work (
    id UUID PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    creation_date DATE,
    certificate TEXT,
    file_path TEXT,
    rating FLOAT,
    type TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Таблица для участников (актёров, режиссёров, сценаристов)
CREATE TABLE IF NOT EXISTS content.person (
    id UUID PRIMARY KEY,
    full_name TEXT NOT NULL,
    birth_date DATE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Таблица для связи жанров и кинопроизведений
CREATE TABLE IF NOT EXISTS content.genre_film_work (
    id UUID PRIMARY KEY,
    film_work_id UUID NOT NULL,
    genre_id UUID NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_film_work FOREIGN KEY (film_work_id)
        REFERENCES content.film_work (id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    CONSTRAINT fk_genre FOREIGN KEY (genre_id)
        REFERENCES content.genre (id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

-- Таблица для связи участников и кинопроизведений
CREATE TABLE IF NOT EXISTS content.person_film_work (
    id UUID PRIMARY KEY,
    film_work_id UUID NOT NULL,
    person_id UUID NOT NULL,
    role TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_film_work_person FOREIGN KEY (film_work_id)
        REFERENCES content.film_work (id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    CONSTRAINT fk_person FOREIGN KEY (person_id)
        REFERENCES content.person (id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

-- Индекс для уникальности жанра и кинопроизведения
CREATE UNIQUE INDEX IF NOT EXISTS film_work_genre
    ON content.genre_film_work (film_work_id, genre_id);

-- Индекс для уникальности участника, кинопроизведения и роли
CREATE UNIQUE INDEX IF NOT EXISTS film_work_person_role
    ON content.person_film_work (film_work_id, person_id, role);
