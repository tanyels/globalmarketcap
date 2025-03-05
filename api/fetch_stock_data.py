import os
import asyncio
import aiohttp
import json
from supabase import create_client
from http.server import BaseHTTPRequestHandler

async def fetch_stock_data_async(symbol, api_key):
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.json()

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

async def fetch_and_store_async(symbol, market, table_name, supabase, api_key):
    raw_data = await fetch_stock_data_async(symbol, api_key)
    parsed_data = parse_stock_data(raw_data, symbol)
    insert_stock_data(parsed_data, table_name, supabase)
    return len(parsed_data)

class Handler(BaseHTTPRequestHandler):
    async def do_GET(self):
        # Load environment variables
        supabase_url = os.environ.get("SUPABASE_URL")
        supabase_key = os.environ.get("SUPABASE_KEY")
        alpha_vantage_api_key = os.environ.get("ALPHA_VANTAGE_API_KEY")
        
        # Check if environment variables are set
        if not all([supabase_url, supabase_key, alpha_vantage_api_key]):
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response_data = {
                "error": "Environment variables not properly configured"
            }
            self.wfile.write(json.dumps(response_data).encode())
            return
        
        try:
            # Initialize Supabase client
            supabase = create_client(supabase_url, supabase_key)
            
            # Fetch and store data for each stock asynchronously
            results = await asyncio.gather(
                fetch_and_store_async("AAPL", "NASDAQ", "nasdaq_historical_prices", supabase, alpha_vantage_api_key),
                fetch_and_store_async("MSFT", "NASDAQ", "nasdaq_historical_prices", supabase, alpha_vantage_api_key),
                fetch_and_store_async("IBM", "NYSE", "nyse_historical_prices", supabase, alpha_vantage_api_key),
                fetch_and_store_async("TSCO.L", "LSE", "lse_historical_prices", supabase, alpha_vantage_api_key)
            )

            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response_data = {
                "status": "success",
                "records_inserted": dict(zip(["AAPL", "MSFT", "IBM", "TSCO.L"], results)),
                "message": "Data inserted successfully!"
            }
            self.wfile.write(json.dumps(response_data).encode())

        except Exception as e:
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response_data = {
                "error": str(e)
            }
            self.wfile.write(json.dumps(response_data).encode())

# This is the format Vercel expects
def handler(event, context):
    return Handler
