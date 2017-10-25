"""
    Extract the last 5 years info's from https://blockchain.info/charts and save in a SQLite3 database
"""
__author__ = 'daniel.adriano'

import io
import requests
import pandas as pd
import sqlite3


def get_blockchaininfo_urls():
    url_base = "https://api.blockchain.info/charts/{0}?timespan=5years&format=csv"

    charts = ["total-bitcoins", "market-price", "market-cap", "trade-volume", "blocks-size",
              "avg-block-size", "n-orphaned-blocks", "n-transactions-per-block",
              "median-confirmation-time", "bip-9-segwit", "bitcoin-unlimited-share",
              "nya-support", "hash-rate", "pools", "difficulty", "miners-revenue",
              "transaction-fees", "transaction-fees-usd", "cost-per-transaction-percent",
              "cost-per-transaction", "n-unique-addresses", "n-transactions", "n-transactions-total",
              "transactions-per-second", "mempool-count", "mempool-growth", "mempool-size",
              "mempool-state-by-fee-level", "utxo-count", "n-transactions-excluding-popular",
              "n-transactions-excluding-chains-longer-than-100", "output-volume", "estimated-transaction-volume",
              "estimated-transaction-volume-usd", "my-wallet-n-users"]
    
    return [url_base.format(url) for url in charts], charts


def get_columns(charts):
    return [column.replace("-", "_") for column in charts]


def get_data(conn, sql):
    data = pd.read_sql_query(sql, conn)
    return data
    

urls, charts = get_blockchaininfo_urls()
columns = get_columns(charts)

conn = sqlite3.connect("crypto.db")

csv = requests.get(urls[0]).content
data = pd.read_csv(io.StringIO(csv.decode('utf-8')), names=["date", columns[0]])

for i, url in enumerate(urls):
    print(charts[i])
    if i == 0:
        continue
    csv = requests.get(url).content
    data_tmp = pd.read_csv(io.StringIO(csv.decode('utf-8')), names=["date", columns[i]])
    data_aux = pd.merge(data, data_tmp, how='left', on="date")
    data = data_aux

data['date'] = pd.to_datetime(data['date'])
data.to_sql("bitcoin", conn, if_exists="replace")
