from http.server import BaseHTTPRequestHandler
import os
import requests
from supabase import create_client, Client
from datetime import datetime
import json

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        # Load environment variables
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_KEY")
        alpha_vantage_api_key = os.getenv("ALPHA_VANTAGE_API_KEY")
        
        # Check if environment variables are set
        if not all([supabase_url, supabase_key, alpha_vantage_api_key]):
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({
                "error": "Environment variables not properly configured"
            }).encode())
            return

        try:
            # Initialize Supabase client
            supabase: Client = create_client(supabase_url, supabase_key)
            
            # Fetch and insert data for each stock
            self.fetch_and_insert_data("AAPL", "NASDAQ", "nasdaq_historical_prices", supabase, alpha_vantage_api_key)
            self.fetch_and_insert_data("MSFT", "NASDAQ", "nasdaq_historical_prices", supabase, alpha_vantage_api_key)
            self.fetch_and_insert_data("IBM", "NYSE", "nyse_historical_prices", supabase, alpha_vantage_api_key)
            self.fetch_and_insert_data("TSCO.L", "LSE", "lse_historical_prices", supabase, alpha_vantage_api_key)
            
            # Send success response
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({
                "status": "success",
                "message": "Data inserted successfully!"
            }).encode())
            
        except Exception as e:
            # Handle errors
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({
                "error": str(e)
            }).encode())
    
    def fetch_and_insert_data(self, symbol, market, table_name, supabase, api_key):
        # Fetch historical data from Alpha Vantage
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}"
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

# This makes Vercel recognize the Handler class
def handler(event, context):
    return Handler
