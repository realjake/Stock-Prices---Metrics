from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional
import pandas as pd
import numpy as np
import requests
import logging
import gspread
import yaml
import time


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(message)s')

# Load API key from config file
with open("dev/config.yaml") as file:
    config = yaml.safe_load(file)
    fmp_api_key = config.get("api_key")
    worksheet_name = config.get("worksheet_name")
    sheet_name = config.get("sheet_name")
    refresh_rate = config.get("refresh_rate")
    rate_limit = config.get("rate_limit")

# Authenticate and connect to Google Sheets
gc = gspread.service_account(filename='dev/credentials.json')
sheet = gc.open(worksheet_name).worksheet(sheet_name)

# Load data from Google Sheet to DataFrame
df = pd.DataFrame(sheet.get_all_values()[1:], columns=sheet.get_all_values()[0])
tickers = df["tickers"].dropna().tolist()

# API request function
def fmp_request(
    version: str,
    endpoint: str,
    symbol: str,
    period: Optional[str] = None,
) -> Optional[dict]:
    period_str = f'period={period}&' if period else ''
    symbol_path = f'/{symbol}' if symbol else ''

    # Construct the URL based on API version
    if version == 'v3':
        url = f"https://financialmodelingprep.com/api/{version}/{endpoint}{symbol_path}?{period_str}apikey={fmp_api_key}"
    elif version == 'v4':
        url = f"https://financialmodelingprep.com/api/{version}/{endpoint}{symbol}?apikey={fmp_api_key}"
    else:
        logging.error(f"Unsupported API version: {version}")
        return None
    # Send request and handle response
    response = requests.get(url, timeout=30)
    data = response.json()
    if not data:
        logging.warning(f"No data returned for {symbol} from {endpoint}.")
    return data

# Fetch and store financial data concurrently
def fmp_data():
    endpoints = {'real-time-quote': ('stock/full/real-time-price/', None, 'v3'), 'quote': ('quote', None, 'v3'), 'postpre': ('pre-post-market/', None, 'v4')}
    financial_data = {symbol: {} for symbol in tickers}
    
    # Set optimal number of workers based on trial and error for your environment
    max_workers = min(50, len(tickers) * len(endpoints))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(fmp_request, version, endpoint, symbol, period): (symbol, var_name)
            for var_name, (endpoint, period, version) in endpoints.items()
            for symbol in tickers
        }
        
        # Use as_completed for faster handling of completed requests
        for future in as_completed(futures):
            symbol, var_name = futures[future]
            try:
                data = future.result()
                if isinstance(data, list) and data:
                    df = pd.DataFrame(data)

                elif isinstance(data, dict) and data:
                    df = pd.DataFrame([data])
                    
                else:
                    logging.warning(f"Unexpected or empty data format: {data}")
                    df = pd.DataFrame()       
                
                financial_data[symbol][var_name] = df

            except Exception as e:
                logging.error(f"Error fetching data for {symbol}: {e} with {var_name}")
    
    return financial_data

# Update Google Sheet with financial data using a single batch update
def google_update(financial_data):
    try:
        # Prepare batch updates in a single list
        updates = []
        
        tickers_start_row = df.index[df["tickers"].notnull()][0] + 2
        for i, symbol in enumerate(df['tickers'].dropna(), start=tickers_start_row):
            data = financial_data.get(symbol, {})
            price_df = data.get('real-time-quote', pd.DataFrame())
            quote_df = data.get('quote', pd.DataFrame())
            post_pre_df = data.get('postpre', pd.DataFrame([]))

            # Extract data
            price = price_df.get('fmpLast', ["N/A"])[0] if not price_df.empty and 'fmpLast' in price_df.columns else "N/A"
            day_change_percent = quote_df['changesPercentage'].iloc[0] / 100 if not quote_df.empty and 'changesPercentage' in quote_df.columns else "N/A"
            earnings_date = quote_df['earningsAnnouncement'].iloc[0] if not quote_df.empty and 'earningsAnnouncement' in quote_df.columns else "N/A"
            postpre = post_pre_df['bid'].iloc[0] if not post_pre_df.empty and 'bid' in post_pre_df.columns else "N/A"
            
            # Convert to native Python types for JSON serialization
            price = float(price) if isinstance(price, (int, float, np.integer, np.floating)) else price
            day_change_percent = float(day_change_percent) if isinstance(day_change_percent, (int, float, np.integer, np.floating)) else day_change_percent
            post_pre_price = float(postpre) if isinstance(postpre, (int, float, np.integer, np.floating)) else postpre
            
            # Add to updates list
            updates.extend([
                {"range": f"B{i}", "values": [[price]]},
                {"range": f"C{i}", "values": [[day_change_percent]]},
                {"range": f"D{i}", "values": [[earnings_date]]},
                {"range": f"E{i}", "values": [[post_pre_price]]}

            ])
        
        # Batch update to Google Sheets in one call
        if updates:
            sheet.batch_update(updates)
            logging.info("Google Sheet updated successfully.")
    except Exception as e:
        logging.error(f"Failed to update Google Sheet: {e}")


def main():
    while True:
        start_time = time.time()
        financial_data = fmp_data()
        google_update(financial_data)
        logging.info(f"Program completed in {time.time() - start_time:.2f} seconds.")
        time.sleep(refresh_rate)

if __name__ == "__main__":
    main()