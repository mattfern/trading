# https://pythonprogramming.net/combining-stock-prices-into-one-dataframe-python-programming-for-finance/
import bs4 as bs
import datetime as dt
import os
import pandas_datareader.data as web
import pandas as pd
import pickle
import requests

def save_sp500_tickers():
    resp = requests.get('http://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    soup = bs.BeautifulSoup(resp.text, 'lxml')
    table = soup.find('table', {'class': 'wikitable sortable'})
    tickers = []
    for row in table.findAll('tr')[1:]:
        ticker = row.findAll('td')[0].text
        tickers.append(ticker)

    with open("sp500tickers.pickle","wb") as f:
        pickle.dump(tickers,f)

    return tickers

#save_sp500_tickers()

def get_data_from_yahoo(reload_sp500=False):
    if reload_sp500:
        tickers = save_sp500_tickers()
    else:
        with open("sp500tickers.pickle", "rb") as f:
            tickers = pickle.load(f)
    if not os.path.exists('stock_dfs'):
        os.makedirs('stock_dfs')

    start = dt.datetime(2010, 1, 1)
    end = dt.datetime.now()
    for ticker in tickers:
        # if connection breaks, we'd like to save our progress
        if not os.path.exists('stock_dfs/{}.csv'.format(ticker)) and ticker != "ANDV" and ticker != "BKNG" and ticker != "BHF": # ANDV keeps timing out
            print('Pulling {}'.format(ticker))
            df = web.DataReader(ticker, 'morningstar', start, end)
            df.reset_index(inplace=True)
            df.set_index("Date", inplace=True)
            df = df.drop("Symbol", axis=1)
            df.to_csv('stock_dfs/{}.csv'.format(ticker))
        else:
            print('Already have {}'.format(ticker))

# get_data_from_yahoo()

def compile_data():
    # with open("sp500tickers.pickle","rb") as f:
    #     tickers = pickle.load(f)
    tickers = os.listdir('stock_dfs')
    print(tickers)
    main_df = pd.DataFrame()
    for count, ticker in enumerate(tickers):
        # TickerNameLen = len(ticker)
        # ticker = str(ticker[0:(TickerNameLen-3)])
        df = pd.read_csv('stock_dfs/{}'.format(ticker))
        df.set_index('Date', inplace=True)
        df.rename(columns={'Close':ticker}, inplace=True)
        df.drop(['High', 'Low', 'Open', 'Volume'],1,inplace=True)
        if main_df.empty:
            main_df = df
        else:
            main_df = main_df.join(df, how='outer')

        if count % 10 == 0:
            print(count)
    print(main_df.head())
    main_df.to_csv('sp500_joined_closes.csv')

compile_data()
