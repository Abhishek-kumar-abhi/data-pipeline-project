
import pandas as pd
from fastapi import FastAPI

app = FastAPI()

@app.get("/customers")
def read_customers():
    """
    Reads customer data from a CSV file and returns it as a JSON response.

    Returns:
        A JSON response containing the customer data.
    """
    try:
        df = pd.read_csv("datasets/olist_customers_dataset.csv")
        return df.to_dict(orient="records")
    except FileNotFoundError:
        return {"error": "File not found"}
    except Exception as e:
        return {"error": str(e)}
