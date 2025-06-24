import os
import time
from datetime import datetime
#from tqdm import tqdm
import pandas as pd
#import dask.dataframe as dd
import numpy as np
import asyncio
from tardis_dev import datasets
from creds import tardis_api_key

def sample_query(start_date:str,end_date:str):

    sample = datasets.download(
        exchange="deribit",
        data_types=[
            "book_snapshot_25",
        ],
        from_date="2019-11-01",
        to_date="2019-11-02",
        symbols=["BTC-PERPETUAL", "ETH-PERPETUAL"],
        api_key=tardis_api_key,
    )
"""
def lob_query(exchange:str,start_date:str,end_date:str,symbols:list=["BTC-PERPETUAL"]):

    try:
        datasets.download(
            exchange=exchange,
            data_types=[
                "book_snapshot_25",
            ],
            from_date=start_date,
            to_date=end_date,
            symbols=symbols,
            api_key=tardis_api_key,
        )
    except Exception as e:
        print(f"Failed for {start_date}: {e}")
"""
    #file_path=f"datasets/deribit_book_snapshot_25_2019-11-01_BTC-PERPETUAL.csv.gz"
    #df = pd.read_csv(file_path, compression='gzip',nrows=10_000)
    #df = pd.read_csv(file_path, compression='gzip')

# Async version of lob_query
async def lob_query_async(exchange: str, date: str, symbols: list):
    
    try:
        await asyncio.to_thread(
            datasets.download(
            exchange=exchange,
            data_types=["book_snapshot_25"],
            from_date=date,
            to_date=date,
            symbols=symbols,
            api_key=tardis_api_key,
            )
        )
    except Exception as e:
        print(f"Download failed for {date}: {e}")
"""
def get_lob_history(exchange:str,start_date:str,end_date:str,symbols:list=["BTC-PERPETUAL"]):
    
    start_time = time.time()
    for day in pd.date_range(start_date, end_date):
        day = day.strftime('%Y-%m-%d')  # convert Timestamp to string
        try:
            lob_query(exchange="deribit",start_date=day,end_date=day,symbols=symbols)
            file_path=f"datasets/{exchange}_book_snapshot_25_{day}_{symbols[0]}.csv.gz"
            df = pd.read_csv(file_path, compression='gzip')
            print(day,df.shape)
            # do other analysis....
            os.remove(file_path)
        except:
            pass
    end_time = time.time() # Record the end time
    elapsed_time = end_time - start_time
    print(f"Elapsed time: {elapsed_time:.4f} seconds")
"""

# Stream + process file in memory without loading full DataFrame
def process_lob_stream(exchange: str, date: str, symbol: str):
    
    file_path = f"datasets/{exchange}_book_snapshot_25_{date}_{symbol}.csv.gz"
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return

    count = 0
    try:
        for msg in datasets.stream(file_path):
            # Example: count snapshot entries or do any light processing
            count += 1
            # You could extract e.g., best bid/ask levels
            # best_bid = msg.get('bids[0].price', None)

        print(f"{date} {symbol}: {count} messages")
    except Exception as e:
        print(f"Failed to stream {file_path}: {e}")

    # Remove file after processing
    os.remove(file_path)


def process_lob_file(file_path: str, date: str, symbol: str):
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return

    count = 0
    try:
        # Option 1
        # Stream read in chunks to reduce memory usage
        chunks = pd.read_csv(file_path, compression='gzip', chunksize=50_000)
        for chunk in chunks:
            count += len(chunk)  # Example: count total rows
            # Do more processing here if needed
        print(f"{date} {symbol}: {count} rows")
    except Exception as e:
        print(f"Error reading {file_path}: {e}")

    os.remove(file_path)

"""
# Orchestrator: download and stream-process LOB data asynchronously
async def get_lob_history(exchange: str, start_date: str, end_date: str, symbols: list = ["BTC-PERPETUAL"]):
    dates = pd.date_range(start=start_date, end=end_date).strftime('%Y-%m-%d').tolist()

    # Download all files asynchronously in batches
    tasks = []
    for date in dates:
        for symbol in symbols:
            file_path = f"datasets/{exchange}_book_snapshot_25_{date}_{symbol}.csv.gz"
            if not os.path.exists(file_path):
                tasks.append(lob_query_async(exchange=exchange, date=date, symbols=[symbol]))

    print(f"Downloading {len(tasks)} files...")
    await asyncio.gather(*tasks)

    # Process downloaded files (stream-based)
    #for date in tqdm(dates, desc="Processing files"):
    for date in dates:
        for symbol in symbols:
            process_lob_stream(exchange, date, symbol)
"""

async def get_lob_history(exchange: str, start_date: str, end_date: str, symbols: list = ["BTC-PERPETUAL"]):
    dates = pd.date_range(start=start_date, end=end_date).strftime('%Y-%m-%d').tolist()
    download_tasks = []

    for date in dates:
        for symbol in symbols:
            file_path = f"datasets/{exchange}_book_snapshot_25_{date}_{symbol}.csv.gz"
            if not os.path.exists(file_path):
                download_tasks.append(lob_query_async(exchange, date, [symbol]))

    print(f"Downloading {len(download_tasks)} files...")
    await asyncio.gather(*download_tasks)

    #for date in tqdm(dates, desc="Processing files"):
    for date in dates:
        for symbol in symbols:
            file_path = f"datasets/{exchange}_book_snapshot_25_{date}_{symbol}.csv.gz"
            process_lob_file(file_path, date, symbol)



#file_path = "datasets/deribit_trades_2019-11-01_BTC-PERPETUAL.csv.gz"