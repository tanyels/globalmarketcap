from flask import Flask, Response
import os
import requests
import json
from supabase import create_client

app = Flask(__name__)

def fetch_stock_data(symbol, api_key):
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}"
    response = requests.get(url)
    return response.json()

def parse_stock_data(data, symbol):
    time_series = data.get("Time Series (Daily)", {})
    parsed_data = []
    
    for date, values in time_series.items():
        parsed_data.append({
            "symbol": symbol,
            "date": date,
            "open": float(values["1. open"]),
            "high": float(values["2. high"]),
            "low": float(values["3. low"]),
            "close": float(values["4. close"]),
            "volume": int(values["5. volume"])
        })
    
    return parsed_data

def insert_stock_data(data_list, table_name, supabase):
    for data in data_list:
        supabase.table(table_name).insert(data).execute()

def fetch_and_store(symbol, market, table_name, supabase, api_key):
    raw_data = fetch_stock_data(symbol, api_key)
    parsed_data = parse_stock_data(raw_data, symbol)
    insert_stock_data(parsed_data, table_name, supabase)
    return len(parsed_data)

@app.route('/api/fetch_stock_data', methods=['GET'])
def fetch_stock_data_handler():
    # Load environment variables
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_KEY")
    alpha_vantage_api_key = os.environ.get("ALPHA_VANTAGE_API_KEY")
    
    # Check if environment variables are set
    if not all([supabase_url, supabase_key, alpha_vantage_api_key]):
        return Response(
            response=json.dumps({"error": "Environment variables not properly configured"}),
            status=500,
            mimetype='application/json'
        )
    
    try:
        # Initialize Supabase client
        supabase = create_client(supabase_url, supabase_key)
        
        # Fetch and store data for each stock
        results = {
            "AAPL": fetch_and_store("AAPL", "NASDAQ", "nasdaq_historical_prices", supabase, alpha_vantage_api_key),
            "MSFT": fetch_and_store("MSFT", "NASDAQ", "nasdaq_historical_prices", supabase, alpha_vantage_api_key),
            "IBM": fetch_and_store("IBM", "NYSE", "nyse_historical_prices", supabase, alpha_vantage_api_key),
            "TSCO.L": fetch_and_store("TSCO.L", "LSE", "lse_historical_prices", supabase, alpha_vantage_api_key)
        }
        
        return Response(
            response=json.dumps({
                "status": "success",
                "records_inserted": results,
                "message": "Data inserted successfully!"
            }),
            status=200,
            mimetype='application/json'
        )
        
    except Exception as e:
        return Response(
            response=json.dumps({"error": str(e)}),
            status=500,
            mimetype='application/json'
        )

# Vercel needs this
@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def catch_all(path):
    return Response(
        response=json.dumps({"error": "Invalid endpoint"}),
        status=404,
        mimetype='application/json'
    )
