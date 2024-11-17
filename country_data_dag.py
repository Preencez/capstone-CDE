from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
from requests.exceptions import Timeout, ConnectionError, SSLError, RequestException
from datetime import datetime

def fetch_country_data():
    try:
        response = requests.get("https://restcountries.com/v3.1/all", timeout=10)
        response.raise_for_status()  # Raise an error for 4xx/5xx HTTP status codes
        countries_data = response.json()
        return countries_data
    except Timeout:
        print("The request timed out.")
    except ConnectionError:
        print("There was a connection error.")
    except SSLError:
        print("There was an SSL error.")
    except RequestException as e:
        print(f"An error occurred: {e}")

def print_countries():
    countries = fetch_country_data()
    if countries:
        print(countries[:5])  # Print the first 5 countries

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 11, 16),
    'retries': 1,
}

with DAG(
    'country_data_dag',
    default_args=default_args,
    description='A simple DAG to fetch country data',
    schedule_interval='@daily',  # Change this based on your needs
    catchup=False,
) as dag:

    fetch_countries_task = PythonOperator(
        task_id='fetch_countries',
        python_callable=print_countries,
    )

fetch_countries_task
