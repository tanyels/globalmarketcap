import os
import requests
from supabase import create_client, Client
from datetime import datetime

# Load environment variables
supabase_url = os.getenv("SUPABASE_URL")
supabase_key = os.getenv("SUPABASE_KEY")
alpha_vantage_api_key = os.getenv("ALPHA_VANTAGE_API_KEY")

# Initialize Supabase client
supabase: Client = create_client(supabase_url, supabase_key)

def fetch_and_insert_data(symbol, market, table_name):
    # Fetch historical data from Alpha Vantage
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={alpha_vantage_api_key}"
    response = requests.get(url)
    data = response.json()

    # Parse and insert data into the appropriate table
    time_series = data.get("Time Series (Daily)", {})
    for date, values in time_series.items():
        supabase.table(table_name).insert({
            "symbol": symbol,
            "date": date,
            "open": float(values["1. open"]),
            "high": float(values["2. high"]),
            "low": float(values["3. low"]),
            "close": float(values["4. close"]),
            "volume": int(values["5. volume"])
        }).execute()

def handler(event, context):
    # Example usage
    fetch_and_insert_data("AAPL", "NASDAQ", "nasdaq_historical_prices")
    fetch_and_insert_data("MSFT", "NASDAQ", "nasdaq_historical_prices")
    fetch_and_insert_data("IBM", "NYSE", "nyse_historical_prices")
    fetch_and_insert_data("TSCO.L", "LSE", "lse_historical_prices")

    return {
        "statusCode": 200,
        "body": "Data inserted successfully!"
    }
