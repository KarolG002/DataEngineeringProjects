import requests
import creds
import pandas as pd
import datetime
import os
import json
from prefect import flow, get_run_logger, task
import psycopg2
#CONSTANTS
API_URL = "http://www.omdbapi.com/"
RECORDS_PER_HOUR = 10
STATE_FILE = "pagination_state.json"
#new
@task
def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as file:
            return json.load(file)
    return {"last_index": 0, "last_date": ""}
@task
def save_state(last_index, last_date):
    with open(STATE_FILE, "w") as file:
        json.dump({"last_index": last_index, "last_date": last_date}, file)
@task
def get_data_from_csv() -> list:
    movie_data_csv = pd.read_csv(f"{creds.filepath}")
    title_year_column =  movie_data_csv.title
    movie_title_year_df = pd.DataFrame(title_year_column, columns=['title'])
    movie_title_year_df[['title','year']] =  movie_title_year_df['title'].str.extract(r'^(.*)\s+\((\d{4})\)$')
    title_list = list(movie_title_year_df.title)
    year_list = list(movie_title_year_df.year)
    return title_list, year_list


@task
def retrieve_movie_from_api(title: str, year: str) -> dict:
    logger = get_run_logger()
    url = f"{API_URL}?t={title}&y={year}&apikey={creds.API_KEY}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        response.encoding = 'utf-8' 
        movie_data_dict = response.json()
        logger.info(movie_data_dict)
        return movie_data_dict
    except UnicodeDecodeError as e:
        logger.error(f"Unicode decode error for {title} ({year}): {e}")
        return None
    except requests.RequestException as e:
        logger.error(f"Request error for {title} ({year}): {e}")
        return None


@task
def clean_movie_data(movie_data_dict: dict) -> dict:
    ratings_list = movie_data_dict.get("Ratings", [])
    imdb_rating = None
    for rating in ratings_list:
        if rating.get("Source") == "Internet Movie Database":
            imdb_rating = rating.get("Value")
            imdb_rating = imdb_rating.split('/')[0]
            break
    runtime_raw = movie_data_dict.get("Runtime", "0")
    runtime_cleaned = None
    if runtime_raw:
        runtime_cleaned = runtime_raw.replace(" min", "").strip()
    
    cleaned_data = {
        "title": movie_data_dict.get("Title", "0"),
        "release_year": int(movie_data_dict.get("Year", 0)),
        "rated": movie_data_dict.get("Rated", "0"),
        "runtime": int(runtime_cleaned),
        "genre": movie_data_dict.get("Genre", "0"),
        "country": movie_data_dict.get("Country", "0"),
        "awards": movie_data_dict.get("Awards", "0"),
        "ratings_imdb": float(imdb_rating) if imdb_rating else "0",
    }

    if cleaned_data["title"] == "0":
        return None

    return cleaned_data
@task
def insert_to_db(
    cleaned_data: dict,
    db_host: str,
    db_user: str,
    db_pass: str,
    db_name: str,
):
    query = """
    INSERT INTO movies (
        title,
        release_year,
        rated,
        runtime,
        genre,
        country,
        awards,
        ratings_imdb
    ) VALUES (
        %(title)s,
        %(release_year)s,
        %(rated)s,
        %(runtime)s,
        %(genre)s,
        %(country)s,
        %(awards)s,
        %(ratings_imdb)s
    )
    """
    try:
        with psycopg2.connect(
            user=db_user, password=db_pass, dbname=db_name, host=db_host
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, cleaned_data)
                connection.commit()
    except psycopg2.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

@flow
def process_movies( 
    db_host: str="localhost",
    db_user: str="root",
    db_pass: str="root",
    db_name: str="moviesdb",
):
    state = load_state()
    last_index = state["last_index"]
    last_date = state["last_date"]

    today = datetime.date.today().strftime("%Y-%m-%d")

    if today != last_date:
        last_index = 0
    
    title_list, year_list = get_data_from_csv()
    
    end_index = min(last_index + RECORDS_PER_HOUR, len(title_list))

    for i in range(last_index, end_index):
        movie_title = title_list[i]
        movie_year = year_list[i]
        try:
            print(f"Fetching data for: {movie_title} made in {movie_year}")
            movie_data_dict = retrieve_movie_from_api(movie_title, movie_year)
            cleaned_data = clean_movie_data(movie_data_dict)
            if cleaned_data is None:
                print(f"Skipping record for {movie_title} due to missing title or other missing data")
                continue
            print(cleaned_data)
            insert_to_db(cleaned_data, db_host, db_user, db_pass, db_name,)
            print(f"Inserted data for: {movie_title}")
        except requests.RequestException as e:
            print(f"Error fetching data for {movie_title}: {e}")
        except Exception as e:
            print(f"Error processing data for {movie_title}: {e}")

        last_index = i + 1
    save_state(last_index, today)

def main():
    process_movies.serve(
        name="movie-collection-deployment",
        interval=datetime.timedelta(hours=1),
)
if __name__ == "__main__":
    main()


