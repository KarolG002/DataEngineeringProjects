 SELECT 'CREATE DATABASE moviesdb'
 WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'moviesdb')\gexec

CREATE TABLE IF NOT EXISTS movies (
    id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    release_year INT NOT NULL,
    rated VARCHAR(255) NOT NULL,
    runtime INT NOT NULL,
    genre VARCHAR(255) NOT NULL,
    country VARCHAR(255) NOT NULL,
    awards VARCHAR(255) NOT NULL,
    ratings_imdb DECIMAL(3,1) NOT NULL
);
      