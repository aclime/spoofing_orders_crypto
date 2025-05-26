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

    #return sample

