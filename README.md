# Movie Data ETL Project

This project implements an ETL (Extract, Transform, Load) pipeline to fetch, process, and store movie data using Prefect for orchestration, Docker for containerization, and PostgreSQL as the database.

## Project Architecture

![FilmFetches_Postgres](images/Architecture_diagram.png "Architecture diagram")

## Components

### 1. Movies Database API
- **Description**: The source of movie data.
- **Role**: Provides movie details via API endpoint.

### 2. ETL Script
- **Description**: A Python script to handle the ETL process.
- **Role**: Extracts data from the API, transforms it, and loads it into the PostgreSQL database.

### 3. Prefect
- **Description**: A workflow orchestration tool.
- **Role**: Manages the execution of ETL tasks, including scheduling and monitoring.

### 4. Docker
- **Description**: A containerization platform.
- **Role**: Hosts the PostgreSQL database in an isolated environment.

### 5. PostgreSQL Server
- **Description**: A relational database management system.
- **Role**: Stores the processed movie data.

## Setup and Installation

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/KarolG002/FilmFetcher_Postgres.git
2. **Set up docker**:
   Ensure Docker is installed on your system.
   Start the PostgreSQL container:
   docker compose up -d

3. **Set Up Prefect**:
   (https://docs.prefect.io/latest/getting-started/installation/)
   Start Prefect server:
   prefect server start

4. **Create a creds.py file with your API key and CSV file path with movies you want to search**:

    API_KEY = 'your_api_key'

    filepath = 'path_to_your_csv_file.csv'

