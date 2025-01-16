# stock_market_etl_pipeline_with_airflow

Stock Market Data Pipeline

This project demonstrates a data pipeline for fetching, transforming, and loading stock market data, built using Apache Airflow. The pipeline is designed to automate the process of gathering raw stock data from an API, cleaning and transforming it, and loading the final processed data into a PostgreSQL database.
Features

    Fetches stock market data from a public API.
    Transforms raw data to clean and enrich it with calculated metrics.
    Loads the processed data into a PostgreSQL database for further analysis or visualization.

Technologies Used

    Apache Airflow: Orchestrates the pipeline with task scheduling and monitoring.
    Python: Implements data fetching, transformation, and loading logic.
    PostgreSQL: Serves as the destination database for the transformed data.
    MinIO (optional): Provides object storage for intermediate data files.

Getting Started

Follow these steps to set up and run the project locally.
Prerequisites

    Python 3.8+
    Docker and Docker Compose (for Airflow and PostgreSQL setup)
    Postgres Client like pgAdmin or DBeaver (if you like)
    Basic knowledge of Airflow and Python scripting

Installation

    Clone this repository:

cd stock_market_etl_pipeline_with_airflow

Build the image with the Dockerfile with the command:

docker build . -- extended_airflow (you can change the name of the image to your liking)

Start the required services using Docker Compose:

docker-compose up -d

Pipeline Workflow
1. Fetch Data

    Task: fetch_stock_data
    Retrieves stock market data from a public API and stores it locally in JSON format.

2. Transform Data

    Task: transform_stock_data
    Cleans and formats the raw stock data, calculates key metrics, and outputs the data as a CSV file.

3. Load Data

    Task: load_to_postgres
    Loads the transformed data into a PostgreSQL database for further use.

Configuration
Environment Variables

The pipeline uses environment variables for sensitive information like database credentials. Create a .env file in the project root with the following values:

ALPHA_VANTAGE_API_KEY="your_stock_api_key"

Airflow Connections

Set up connections in Airflow for the database and API. Use the Airflow UI (Admin > Connections) to add:

    'Connection id'=postgres
    'Connection Type'=Postgres
    'Host'=host.docker.internal
    'Database'=airflow
    'Login'=your_username
    'Password'=your_password
    
For your Postgres UI client (like DBeaver or pgAdmin) connection settings to check your database and tables. Create a database with the credentials as follows

    DB_HOST=localhost
    DB_PORT=5432
    Database=airflow
    DB_USER=your_username
    DB_PASSWORD=your_password

    Postgres Connection: For loading data into the database.
    HTTP Connection: For the stock market API.

Running the Pipeline

    Access the Airflow web UI at http://localhost:8080.
    Enable the DAG named Stock Market Data Pipeline.
    Trigger the DAG manually or let it run on its schedule.

Project Structure

stock-market-data-pipeline/
│
├── dags/
│   └── stockmarket_pipeline.py  # Python script to fetch, clean and transform data, and Airflow DAG definitions
├── docker-compose.yml           # Docker Compose setup
├── Dockerfile                   # Dockerfile
├── requirements.txt             # Python dependencies
└── README.md                    # Project documentation


Please delete the stock_data.json and stock_data.csv before running your pipeline. (Just an example of some output)


Future Enhancements

    Add MinIO integration for object storage of raw and processed data.
    Implement sensors for data availability and system health checks.
    Introduce data validation checks during the transformation stage.
    Expand the pipeline to include visualization dashboards.
