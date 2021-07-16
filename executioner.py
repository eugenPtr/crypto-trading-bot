import pandas as pd
import bisnita
from constants import *

"""
This class will be used for fetching data from the exchange, 
building datasets and executing orders
"""


class Executioner:

    def __init__(self, client, open_time_of_last_kline):
        self._client = client
        self._open_time_of_last_kline = open_time_of_last_kline

    def get_traded_tokens_balance(self):

        '''
        :return: The balance of all the tokens you are trading on the exchange
        '''

        wallet = {}
        for token in TRADED_TOKENS:
            balance = self._client.get_asset_balance(token)
            wallet[token] = float(self._client.get_asset_balance(token)['free'])
        return wallet

    # Builds dataset used to bootstrap the model on startup
    def build_bootstrap_dataset(self, file_path=DATA_FILEPATH):

        '''
        Builds a dataset using stored data, plus latest data which hasn't been stored yet. This dataset
        is used to bootstrap the model. Stores the new data along the way
        :return: bootstrap dataset
        '''

        klines_df_dict = {}
        open_time_of_last_kline = 0
        for token in TRADED_TOKENS:
            klines_df = self._get_bootstrap_klines_df(token, file_path + token + ".csv")
            open_time_of_last_kline = int(klines_df.iloc[-1]['OpenTime'])
            klines_df['OpenTime'] = pd.to_datetime(klines_df['OpenTime'], unit='ms')
            klines_df.set_index('OpenTime', drop=True, inplace=True)
            klines_df_dict[token] = klines_df

        self._open_time_of_last_kline = open_time_of_last_kline
        dataset = bisnita.build_dataset(klines_df_dict)
        return dataset


    def build_and_save_new_dataset(self, file_path=DATA_FILEPATH):

        '''
        This function gets latest data, stores it and builds a dataset
        :param file_path:
        :return: dataset containing latest price data
        '''

        klines_df_dict = {}
        open_time_of_last_kline = 0
        for token in TRADED_TOKENS:
            start_time = self._open_time_of_last_kline + 60000
            df = self._get_and_save_latest_klines_dataframe(token, file_path + token + ".csv", start_time)
            open_time_of_last_kline = int(df.iloc[-1]['OpenTime'])
            df['OpenTime'] = pd.to_datetime(df['OpenTime'], unit='ms')
            df.set_index('OpenTime', drop=True, inplace=True)
            klines_df_dict[token] = df

        self._open_time_of_last_kline = open_time_of_last_kline
        dataset = bisnita.build_dataset(klines_df_dict)
        return dataset

    def get_quote_token_balance(self):

        '''

        :return: the reference token used as pair for the traded tokens. i.e. USDT in BTCUSDT
        '''
        return float(self._client.get_asset_balance(QUOTE_TOKEN)['free'])

    ############ PRIVATE METHODS ####################################

    def _get_and_save_latest_klines_dataframe(self, token, filename, start_time):

        klines = self._get_token_klines_since_time(token, start_time)
        df = pd.DataFrame(columns=COLS_IN_USE)
        while klines:
            new_df = pd.DataFrame(klines, columns=COL_NAMES)
            new_df = new_df[COLS_IN_USE]
            new_df.to_csv(filename, index=False, header=None, mode="a")
            frames = [df, new_df]
            df = pd.concat(frames, sort=False)
            start_time = int(new_df.iloc[-1]['OpenTime']) + 60000
            klines = self._get_token_klines_since_time(token, start_time)

        return df

    def _get_bootstrap_klines_df(self, token, filename):

        # Get stored data or restore historic data
        try:
            df = pd.read_csv(filename, header=None, names=COL_NAMES)
        except:
            # TODO: Replace fetching last 1k minutes with fetching all data since a given date and time
            klines = self._client.get_historical_klines(token + QUOTE_TOKEN, self._client.KLINE_INTERVAL_1MINUTE,
                                                        "1000 minutes ago UTC")
            df = pd.DataFrame(klines, columns=COL_NAMES)
            df.to_csv(filename, index=False, header=False)

        df = df[COLS_IN_USE]
        start_time = int(df.iloc[-1]['OpenTime']) + 60000

        new_df = self._get_and_save_latest_klines_dataframe(token, filename, start_time)
        frames = [df, new_df]
        df = pd.concat(frames, sort=False)

        return df


    def _get_token_klines_since_time(self, token, time_as_ms):
        klines = self._client.get_klines(
            symbol=token + QUOTE_TOKEN,
            interval=self._client.KLINE_INTERVAL_1MINUTE,
            limit=500,
            startTime=time_as_ms
        )
        return klines

    def market_sell_order(self, symbol, quantity):
        order = self._client.order_market_sell(
            symbol=symbol,
            quantity=quantity)
        return order

    def market_buy_order(self, symbol, quantity):
        order = self._client.order_market_sell(
            symbol=symbol,
            quantity=quantity)
        return order

