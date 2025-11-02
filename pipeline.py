
import pandas as pd
import requests
from sqlalchemy import create_engine, text
from prefect import task, flow
from prefect.server.schemas.schedules import IntervalSchedule
from prefect.cache_policies import NO_CACHE
from datetime import timedelta

# --- Connection Settings ---
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "ABHI@7M")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "olist_db")

# URL encode the password to handle special characters
from urllib.parse import quote_plus
DB_PASSWORD_ENCODED = quote_plus(DB_PASSWORD)
DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD_ENCODED}@{DB_HOST}:{DB_PORT}/{DB_NAME}?connect_timeout=10"

if not DB_PASSWORD:
    raise ValueError("Database password not found in environment variables")

# --- Task Definitions ---

@task(retries=2, retry_delay_seconds=30)
def fetch_customers_data():
    """Fetches customer data from CSV if API is not available."""
    try:
        response = requests.get("http://127.0.0.1:8000/customers")
        response.raise_for_status()
        return pd.DataFrame(response.json())
    except Exception as e:
        print(f"Warning: Could not fetch data from API: {str(e)}")
        print("Falling back to CSV file...")
        return read_csv_data("datasets/olist_customers_dataset.csv")

@task(retries=3, retry_delay_seconds=30)
def read_csv_data(file_path: str):
    """Reads data from a CSV file."""
    try:
        # Get the absolute path of the script
        script_dir = os.path.dirname(os.path.abspath(__file__))
        # Construct the absolute path to the CSV file
        abs_path = os.path.join(script_dir, file_path)
        # Verify the file exists
        if not os.path.exists(abs_path):
            raise FileNotFoundError(f"CSV file not found at {abs_path}")
        # Read the CSV file
        df = pd.read_csv(abs_path)
        # Verify the dataframe is not empty
        if df.empty:
            raise ValueError(f"CSV file at {abs_path} is empty")
        return df
    except FileNotFoundError as e:
        raise FileNotFoundError(f"CSV file not found at {abs_path}") from e
    except pd.errors.EmptyDataError as e:
        raise ValueError("CSV file is empty") from e
    except Exception as e:
        raise Exception(f"Error reading CSV file: {str(e)}") from e

@task
def merge_data(customers_df, orders_df, order_items_df, order_payments_df):
    """Merges all data sources into a single DataFrame."""
    df = pd.merge(orders_df, customers_df, on="customer_id")
    df = pd.merge(df, order_items_df, on="order_id")
    df = pd.merge(df, order_payments_df, on="order_id")
    return df

@task
def clean_data(df):
    """Cleans the merged DataFrame."""
    df['order_purchase_timestamp'] = pd.to_datetime(df['order_purchase_timestamp'])
    df['order_approved_at'] = pd.to_datetime(df['order_approved_at'])
    df['order_delivered_carrier_date'] = pd.to_datetime(df['order_delivered_carrier_date'])
    df['order_delivered_customer_date'] = pd.to_datetime(df['order_delivered_customer_date'])
    df['order_estimated_delivery_date'] = pd.to_datetime(df['order_estimated_delivery_date'])
    return df

@task(retries=2, retry_delay_seconds=60)
def load_to_postgres(df, table_name):
    """Loads a DataFrame into a PostgreSQL table."""
    try:
        engine = create_engine(DB_URL)
        # Test connection first
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        
        # If connection test passes, proceed with data load
        with engine.begin() as connection:
            # Create a temporary table first
            temp_table = f"temp_{table_name}"
            df.to_sql(temp_table, connection, if_exists="replace", index=False)
            
            # Then swap the tables atomically
            connection.execute(text(f"""
                DROP TABLE IF EXISTS {table_name};
                ALTER TABLE {temp_table} RENAME TO {table_name};
            """))
            
            # Verify the row count
            result = connection.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            count = result.scalar()
            if count != len(df):
                raise ValueError(f"Row count mismatch. Expected {len(df)}, got {count}")
    except Exception as e:
        print(f"Error details: {str(e)}")
        raise Exception(f"Failed to load data to table {table_name}. See error details above.") from e

@task(retries=2, retry_delay_seconds=60, cache_policy=NO_CACHE)
def create_analytics_tables(db_url: str):
    """Creates analytics-ready tables.

    Note: receive a database URL (string) and create the SQLAlchemy engine inside
    the task. This avoids passing a non-serializable Engine object into Prefect
    (which cannot be hashed/serialized for caching).
    """
    try:
        engine = create_engine(db_url)
        with engine.begin() as connection:
            print("Creating sales summary table...")
            # Sales summary table
            # Create in a transaction to ensure atomicity
            connection.execute(text("""
                DROP TABLE IF EXISTS sales_summary;
                CREATE TABLE sales_summary AS
                SELECT
                    c.customer_unique_id,
                    c.customer_city,
                    c.customer_state,
                    COUNT(o.order_id) AS total_orders,
                    SUM(oi.price) AS total_sales,
                    NOW() as last_updated
                FROM olist_orders_dataset o
                JOIN olist_customers_dataset c ON o.customer_id = c.customer_id
                JOIN olist_order_items_dataset oi ON o.order_id = oi.order_id
                GROUP BY 1, 2, 3;
            """))
            
            print("Creating sales summary index...")
            connection.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_sales_summary_customer 
                ON sales_summary(customer_unique_id);
            """))
            
            # Verify the table was created
            result = connection.execute(text("SELECT COUNT(*) FROM sales_summary"))
            count = result.scalar()
            print(f"Sales summary table created with {count} rows")

            print("Creating delivery performance table...")
            # Delivery performance table
            connection.execute(text("""
                DROP TABLE IF EXISTS delivery_performance;
                CREATE TABLE delivery_performance AS
                SELECT
                    o.order_id,
                    o.order_purchase_timestamp::timestamp,
                    o.order_delivered_customer_date::timestamp,
                    o.order_estimated_delivery_date::timestamp,
                    o.order_delivered_customer_date::timestamp - o.order_purchase_timestamp::timestamp AS delivery_time,
                    o.order_estimated_delivery_date::timestamp - o.order_delivered_customer_date::timestamp AS delivery_diff,
                    NOW() as last_updated
                FROM olist_orders_dataset o
                WHERE o.order_status = 'delivered';
            """))
            
            print("Creating delivery performance index...")
            connection.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_delivery_performance_order 
                ON delivery_performance(order_id);
            """))
            
            # Verify the table was created
            result = connection.execute(text("SELECT COUNT(*) FROM delivery_performance"))
            count = result.scalar()
            print(f"Delivery performance table created with {count} rows")

    except Exception as e:
        print(f"Error details: {str(e)}")
        raise Exception("Failed to create analytics tables. See error details above.") from e


# --- Flow Definition ---

@flow(name="Olist Data Pipeline")
def olist_data_pipeline():
    """The main data pipeline flow."""
    try:
        print("Starting data pipeline...")
        
        # 1. Read data
        print("Reading data files...")
        customers_df = fetch_customers_data()
        print(f"Loaded {len(customers_df)} customer records")
        
        orders_df = read_csv_data("datasets/olist_orders_dataset.csv")
        print(f"Loaded {len(orders_df)} order records")
        
        order_items_df = read_csv_data("datasets/olist_order_items_dataset.csv")
        print(f"Loaded {len(order_items_df)} order item records")
        
        order_payments_df = read_csv_data("datasets/olist_order_payments_dataset.csv")
        print(f"Loaded {len(order_payments_df)} payment records")

        # Test database connection
        print("\nTesting database connection...")
        engine = create_engine(DB_URL)
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("Database connection successful!")
        except Exception as e:
            raise Exception(f"Database connection failed: {str(e)}")

        # 2. Load raw data to postgres
        print("\nLoading data to PostgreSQL...")
        load_to_postgres(customers_df, "olist_customers_dataset")
        print("Loaded customers data")
        load_to_postgres(orders_df, "olist_orders_dataset")
        print("Loaded orders data")
        load_to_postgres(order_items_df, "olist_order_items_dataset")
        print("Loaded order items data")
        load_to_postgres(order_payments_df, "olist_order_payments_dataset")
        print("Loaded payments data")

        # 3. Create analytics tables
        print("\nCreating analytics tables...")
        # Pass DB_URL (string) instead of a SQLAlchemy Engine to avoid sending
        # a non-serializable object into the Prefect task (which breaks hashing).
        create_analytics_tables(DB_URL)
        print("Analytics tables created successfully!")
        
        print("\nPipeline completed successfully!")
        return True
    except Exception as e:
        print(f"\nError in pipeline: {str(e)}")
        raise


if __name__ == "__main__":
    try:
        # Run the pipeline directly for testing
        olist_data_pipeline()
    except Exception as e:
        print(f"Pipeline execution failed: {str(e)}")
        raise

    # For deployment (uncomment and modify as needed)
    # from prefect.deployments import Deployment
    # from prefect.server.schemas.schedules import IntervalSchedule
    # 
    # deployment = Deployment.build_from_flow(
    #     flow=olist_data_pipeline,
    #     name="olist-data-pipeline-deployment",
    #     version="1",
    #     work_queue_name="default",
    #     schedule=IntervalSchedule(interval=timedelta(hours=1)),
    #     tags=["olist", "etl"],
    # )
    # deployment.apply()
