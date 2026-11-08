import json
import logging
import os
from datetime import datetime, timedelta, timezone
from urllib.request import urlopen

from airflow.sdk import DAG, get_current_context, task

API_URL = "https://api.frankfurter.dev/v2/rates?base=EUR&quotes=USD,GBP,JPY"
EXPECTED_QUOTES = {"USD", "GBP", "JPY"}
RATE_ID_FIELDS = ("base_currency", "target_currency", "rate_date")
logger = logging.getLogger(__name__)


with DAG(
    dag_id="currency_rates_etl",
    description="Fetch daily EUR exchange rates and store them in MongoDB",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["currency", "mongodb"],
) as dag:

    @task(do_xcom_push=False)
    def extract_currency_exchange_rates():
        with urlopen(API_URL, timeout=30) as response:
            rates = json.load(response)

        retrieved_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
        ti = get_current_context()["ti"]
        ti.xcom_push(
            key="raw_rates",
            value={"rates": rates, "retrieved_at": retrieved_at},
        )
        logger.info("Fetched %d rates from Frankfurter", len(rates))

    @task(do_xcom_push=False)
    def transform_exchange_rate_data():
        ti = get_current_context()["ti"]
        payload = ti.xcom_pull(
            task_ids="extract_currency_exchange_rates", key="raw_rates"
        )
        rates = payload["rates"]

        if len(rates) != 3 or {row["quote"] for row in rates} != EXPECTED_QUOTES:
            raise ValueError("Expected exactly one rate each for USD, GBP, and JPY")

        documents = [
            {
                "base_currency": row["base"],
                "target_currency": row["quote"],
                "exchange_rate": float(row["rate"]),
                "rate_date": row["date"],
                "source": "Frankfurter API",
                "retrieved_at": payload["retrieved_at"],
            }
            for row in rates
        ]

        ti.xcom_push(key="transformed_rates", value=documents)
        logger.info("Transformed %d exchange rate documents", len(documents))

    @task(do_xcom_push=False)
    def load_data_into_mongodb():
        from pymongo import MongoClient

        ti = get_current_context()["ti"]
        documents = ti.xcom_pull(
            task_ids="transform_exchange_rate_data", key="transformed_rates"
        )
        if not documents:
            raise ValueError("No transformed rates received")

        with MongoClient(
            host="mongodb",
            port=27017,
            username=os.environ["MONGO_INITDB_ROOT_USERNAME"],
            password=os.environ["MONGO_INITDB_ROOT_PASSWORD"],
            authSource="admin",
            serverSelectionTimeoutMS=10000,
        ) as client:
            collection = client["currency_etl"]["exchange_rates"]
            collection.create_index(
                [(field, 1) for field in RATE_ID_FIELDS], unique=True
            )

            for document in documents:
                rate_id = {field: document[field] for field in RATE_ID_FIELDS}
                collection.update_one(rate_id, {"$set": document}, upsert=True)

        logger.info("Wrote %d rates to currency_etl.exchange_rates", len(documents))

    extract_currency_exchange_rates() >> transform_exchange_rate_data() >> load_data_into_mongodb()