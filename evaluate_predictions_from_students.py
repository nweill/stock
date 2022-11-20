import pandas as pd
import requests
from yahoo_fin import stock_info as si
import random
import logging
from datetime import date
import dask.bag as db

random.seed(42)

def get_snp500_list(size = 5):

    return random.sample(si.tickers_sp500(),size)


def get_prediction(query):
    log = logging.getLogger()
    log.warning(f'running query: {query}')
    name, url, ticker = query
    if 'TICKER' in url:
        ticker = ticker.upper()
        url = url.replace('TICKER','ticker')
    try:

        response = requests.get(url.replace('<ticker>', ticker))
        res = response.content.lower().decode('UTF-8').strip()
        if 'buy' in res:
            pred = 'buy'
        elif 'sell' in res :
            pred = 'sell'
        else:
            pred = None
    except Exception as e:
        log.warning(f'{e}')
        pred = None
    log.warning(f'results: {name, ticker, pred}')
    return name, ticker, pred


if __name__ == '__main__':
    N = 5
    dask = True
    df = pd.read_csv('data/endpoints.csv')
    list_ticker = get_snp500_list(N)
    queries = []
    for ticker in list_ticker:
        # i = 0
        for _, row in df.iterrows():
            # if i == 1:
            #     break
            queries.append((row['name'], row['url'], ticker))
            # i += 1
    random.shuffle(queries)
    if dask:
        b = db.from_sequence(queries)
        preds = b.map(get_prediction).to_dataframe(columns=['name', 'ticker', 'prediction']).compute()
    else:
        preds = map(get_prediction, queries)
        predictions = pd.DataFrame(columns=['name', 'ticker', 'prediction'], data=preds)
    date_formated = date.today().strftime("%d-%m-%Y")
    preds.to_csv(f'predictions-{date_formated}.csv')