# Currency Exchange Rate ETL

A daily Airflow pipeline that fetches EUR exchange rates for USD, GBP, and JPY from the Frankfurter API and stores them in MongoDB. The project runs locally with Astro CLI and Docker. Mongo Express provides a browser interface for viewing the stored records.

## Pipeline

| Task | What it does |
| --- | --- |
| `extract_currency_exchange_rates` | Fetches the latest rates from Frankfurter and pushes the response and retrieval timestamp to the `raw_rates` XCom key. |
| `transform_exchange_rate_data` | Pulls `raw_rates`, validates the three requested currencies, and pushes the transformed records to `transformed_rates`. |
| `load_data_into_mongodb` | Pulls `transformed_rates` and writes the records to `currency_etl.exchange_rates`. |

Each record contains `base_currency`, `target_currency`, `exchange_rate`, `rate_date`, `source`, and `retrieved_at`. MongoDB uses the base currency, target currency, and rate date as a unique key, so rerunning the pipeline for the same rates updates the existing records.

## Docker Setup

MongoDB and Mongo Express run as separate Docker services on the shared `airflow` network. Airflow connects to MongoDB at `mongodb:27017`. The named `mongo_data` volume persists MongoDB data at `/data/db`.

## Run Locally

Install Git, Docker Desktop, and Astro CLI, then run these commands in PowerShell:

```powershell
git clone https://github.com/nihaal237/currency-etl.git
cd currency-etl
Copy-Item .env.mongo.example .env.mongo
```

Edit `.env.mongo` and replace the placeholder passwords. Use the same MongoDB password in `MONGO_INITDB_ROOT_PASSWORD` and `ME_CONFIG_MONGODB_URL`. Set a separate password for the Mongo Express login. Keep `.env.mongo` local.

```powershell
astro dev start
```

1. Open [Airflow](http://localhost:8080), find `currency_rates_etl`, and unpause it if needed.
2. Click **Trigger** to run the DAG manually. Confirm that all three tasks succeed.
3. Open [Mongo Express](http://localhost:8081), sign in with the web credentials from `.env.mongo`, and select `currency_etl` → `exchange_rates`.
4. To inspect the named XCom entries, open the Extract or Transform task instance in Airflow and select **Storage → XCom**.

The DAG is scheduled for midnight UTC (5:00 AM Pakistan time) while the local stack is running and the DAG is unpaused.

To stop the stack and retain the database records, run:

```powershell
astro dev stop
```
