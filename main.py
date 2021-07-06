from binance import Client
from config import TEST_API_KEY, TEST_API_SECRET
from constants import *
from executioner import Executioner
import pandas as pd
import bisnita
import time

if __name__ == '__main__':

    client = Client(TEST_API_KEY, TEST_API_SECRET, testnet=True)
    executioner = Executioner(client, 0)

    dataset = executioner.build_bootstrap_dataset()
    print(dataset)

    spans = [20, 50, 100]
    model = bisnita.BișnițăModel(spans, klines_per_day=60 * 24)
    weights = model.mama_omida(dataset)

    while True:
        time.sleep(60)
        dataset = executioner.build_and_save_new_dataset()
        print(dataset)
        if dataset.empty == False:
            weights = model.mama_omida(dataset)
            prices = dataset['Close'].iloc[-1]

            wallet = executioner.get_traded_tokens_balance()
            sell_token_balance = executioner.get_sell_token_balance()
            portfolio = pd.Series(wallet)

            target = bisnita.următoarea_combinație(weights.iloc[-1], portfolio, sell_token_balance, prices)
            print(target)


            #Sell
            for token in TRADED_TOKENS:
                if wallet[token] > target[token]:
                    print(token + " balance: " + str(wallet[token]))
                    print("target balance: " + str(target[token]))
                    print("Selling " + str(wallet[token] - target[token]) + " " + token)
                    order = executioner.market_sell_order(token+SELL_TOKEN, wallet[token]-target[token])
                    print(order)
                    print("\n")

            wallet = executioner.get_traded_tokens_balance()
            sell_token_balance = executioner.get_sell_token_balance()
            portfolio = pd.Series(wallet)
            target = bisnita.următoarea_combinație(weights.iloc[-1], portfolio, sell_token_balance, prices)

            #Buy
            for token in TRADED_TOKENS:
                if wallet[token] < target[token]:
                    print(token + " balance: " + str(wallet[token]))
                    print("target balance: " + str(target[token]))
                    print("Buying " + str(target[token] - wallet[token]) + " " + token)
                    order = executioner.market_buy_order(token+SELL_TOKEN, target[token]-wallet[token])
                    print(order)
                    print("\n")








