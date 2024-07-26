CREATE TABLE movies (
    id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    release_year INT NOT NULL,
    rated VARCHAR(255) NOT NULL,
    runtime VARCHAR(255) NOT NULL,
    genre VARCHAR(255) NOT NULL,
    country VARCHAR(255) NOT NULL,
    awards VARCHAR(255) NOT NULL,
    ratings_imdb DECIMAL(3,1) NOT NULL,
);
      