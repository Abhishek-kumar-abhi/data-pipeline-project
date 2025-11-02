End-to-End Data Pipeline using FastAPI, PostgreSQL, Prefect & Grafana


Project Overview

This project demonstrates a complete data engineering pipeline using modern open-source tools.
It reads raw data from multiple sources (CSV and JSON API), performs data cleaning and transformation, loads the results into a PostgreSQL database, and visualizes analytics dashboards in Grafana.

The pipeline is automated and orchestrated using Prefect, ensuring hourly updates.

Components Used

FastAPI → Serves JSON API for one dataset

Pandas → For data cleaning and merging

PostgreSQL → For storing and querying processed data

Prefect → For orchestration and hourly scheduling

Grafana → For creating interactive dashboards


Installation & Setup
1️⃣ Clone Repository & Install Requirements
git clone <repo_url>
cd data_pipeline_project
python -m venv venv
source venv/bin/activate        # (Windows: venv\Scripts\activate)
pip install -r requirements.txt

2️⃣ Setup PostgreSQL

Make sure PostgreSQL is running locally.

Example configuration (.env file):

DB_NAME=olist_db
DB_USER=postgres
DB_PASSWORD=ABHI@7M
DB_HOST=localhost
DB_PORT=5432

3️⃣ Run FastAPI Server

Serve one dataset as a JSON API:

cd app
uvicorn main:app --reload


Access it at 👉 http://127.0.0.1:8000/data

4️⃣ Run Prefect Flow

Run the Prefect server:

prefect server start


Deploy and run the data pipeline:

python prefect_flow.py


To schedule every 1 hour:

prefect deployment run "data_pipeline_flow"
prefect agent start

5️⃣ Connect PostgreSQL to Grafana

Go to https://grafana.com/products/cloud/
 → Start for Free

Add a new PostgreSQL data source

Fill credentials:

Host: localhost
Database: analytics_db
User: postgres
Password: postgres
SSL Mode: disable


Click “Save & Test”


Chart Type	Data Source Table	Example Query	Description
Bar Chart	sales_summary	SELECT region, SUM(total_revenue) FROM sales_summary GROUP BY region;	Revenue by region
Pie Chart	sales_summary	SELECT region, COUNT(*) FROM sales_summary GROUP BY region;	Orders per region
Line Chart	staging_data	SELECT order_date, SUM(payment_value) FROM staging_data GROUP BY order_date ORDER BY order_date;	Daily sales trend
Gauge / Stat Panel	delivery_performance	SELECT AVG(avg_payment) FROM delivery_performance;	Average payment value


Pipeline Workflow

Data Extraction:

Reads CSV data from /data/olist_order_payments_dataset.csv

Reads JSON API data from FastAPI endpoint /data

Data Transformation:

Cleans missing values

Merges sources on common keys (e.g., order_id, customer_id)

Data Loading:

Inserts cleaned data into PostgreSQL tables:

staging_data

sales_summary

delivery_performance

Analytics Visualization:

Grafana connects to PostgreSQL and visualizes dashboards.

Orchestration:

Prefect runs this pipeline automatically every 1 hour.

✅ Connection Troubleshooting
Checkpoint	Description
🔹 PostgreSQL running	Start using sudo service postgresql start
🔹 Database credentials correct	Match .env and Grafana settings
🔹 Port open	Ensure port 5432 is accessible
🔹 FastAPI reachable	Visit http://127.0.0.1:8000/data
🔹 Prefect flow active	Check with prefect server start


  Requirements

Example requirements.txt:

fastapi
uvicorn
pandas
sqlalchemy
psycopg2-binary
prefect
python-dotenv

Summary

This project demonstrates:

Data ingestion from mixed sources

Automated data engineering with Prefect

Relational modeling in PostgreSQL

Real-time visualization using Grafana Cloud