import requests
import creds
import pandas as pd
import datetime
import os
import json
from prefect import flow, get_run_logger, task

#CONSTANTS
API_URL = "http://www.omdbapi.com/"
RECORDS_PER_DAY = 5
STATE_FILE = "pagination_state.json"

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
def retrieve_movie_from_api(
        title: str, 
        year: str
) -> dict:
    logger = get_run_logger()
    url=f"{API_URL}?t={title}&y={year}&apikey={creds.API_KEY}"
    response = requests.get(url)
    response.raise_for_status() 
    movie_data_dict = response.json()
    logger.info(movie_data_dict)
    return movie_data_dict

@task
def clean_movie_data(movie_data_dict: dict) -> dict:
    ratings_list = movie_data_dict.get("Ratings", [])
    imdb_rating = None
    for rating in ratings_list:
        if rating.get("Source") == "Internet Movie Database":
            imdb_rating = rating.get("Value")
            imdb_rating = imdb_rating.split('/')[0]
            break
    runtime_raw = movie_data_dict.get("Runtime", 0)
    runtime_cleaned = None
    if runtime_raw:
        runtime_cleaned = runtime_raw.replace(" min", "").strip()

    return {
        "title": movie_data_dict.get("Title", 0),
        "year": movie_data_dict.get("Year", 0),
        "rated": movie_data_dict.get("Rated", 0),
        "runtime": runtime_cleaned,
        "genre": movie_data_dict.get("Genre", 0),
        "country": movie_data_dict.get("Country", 0),
        "awards": movie_data_dict.get("Awards", 0),
        "ratings_imdb": imdb_rating,
    }

@flow
def process_movies():
    state = load_state()
    last_index = state["last_index"]
    last_date = state["last_date"]

    today = datetime.date.today().strftime("%Y-%m-%d")

    if today != last_date:
        last_index = 0
    
    title_list, year_list = get_data_from_csv()
    
    end_index = min(last_index + RECORDS_PER_DAY, len(title_list))

    for i in range(last_index, end_index):
        movie_title = title_list[i]
        movie_year = year_list[i]
        try:
            print(f"Fetching data for: {movie_title} made in {movie_year}")
            movie_data_dict = retrieve_movie_from_api(movie_title, movie_year)
            cleaned_data = clean_movie_data(movie_data_dict)
            print(cleaned_data)
        except requests.RequestException as e:
            print(f"Error fetching data for {movie_title}: {e}")

        last_index = i + 1
    save_state(last_index, today)


def main():
    process_movies.serve(
        name="movie-collection-deployment",
        interval=datetime.timedelta(hours=1),
)
if __name__ == "__main__":
    main()


