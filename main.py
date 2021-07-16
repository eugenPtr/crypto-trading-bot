from binance import Client
from config import TEST_API_KEY, TEST_API_SECRET
from constants import *
from executioner import Executioner
import pandas as pd
import numpy as np
import bisnita
import time
import ccxt
from ccxt.base.errors import InvalidOrder

if __name__ == '__main__':

    exchange = ccxt.binance(
        {'apiKey': TEST_API_KEY,
         'secret': TEST_API_SECRET,
         'timeout': 30000, 'enableRateLimit': True,
    })
    exchange.set_sandbox_mode(True)

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
            # weights = model.mama_omida(dataset)
            prices = dataset['Close'].iloc[-1]

            wallet = executioner.get_traded_tokens_balance()
            quote_token_balance = executioner.get_quote_token_balance()
            portfolio = pd.Series(wallet)

            target = bisnita.următoarea_combinație(weights.iloc[-1], portfolio, quote_token_balance, prices)
            print(target)


            #Sell
            for base_token in TRADED_TOKENS:
                print('USDT balance: %f' % executioner.get_quote_token_balance())
                print('%s balance: %f' % (base_token, wallet[base_token]))
                print('target balance %f \n' % target[base_token])

                if wallet[base_token] > target[base_token]:
                    size = (wallet[base_token] - target[base_token])
                    market_pair = base_token + "/" + QUOTE_TOKEN
                    print('%s balance: %f' % (base_token, wallet[base_token]))
                    print('target balance %f' % target[base_token])
                    print('Selling %f %s' % (size, base_token))
                    try:
                        order_result = exchange.create_market_sell_order(market_pair, size)
                        print('SELL: Succesful %s order at: %f USDT' % (market_pair, size))
                        print(order_result)
                    except InvalidOrder as e:
                        print('SELL: Invalid %s order for size %f USDT' % (market_pair, size))
                        print(e)

            wallet = executioner.get_traded_tokens_balance()
            quote_token_balance = executioner.get_quote_token_balance()
            portfolio = pd.Series(wallet)
            target = bisnita.următoarea_combinație(weights.iloc[-1], portfolio, quote_token_balance, prices)

            # Calculate buy coefficient
            # portfolioValue = (portfolio * prices).sum() + quote_token_balance
            # print("Portfolio value: ", portfolioValue)
            # current_ratio = (portfolio * prices) / portfolioValue
            # amount_to_buy = (weights.iloc[-1] - current_ratio).clip(lower=0).sum() * portfolioValue
            amount_to_buy = (target - (portfolio * prices)).clip(lower=0).sum()
            if amount_to_buy > 0:
                buy_scaling = BUY_SCALING_STATIC * np.clip(quote_token_balance/amount_to_buy, a_max=1.0, a_min=None)

            #Buy
            for base_token in TRADED_TOKENS:
                print('USDT balance: %f' % executioner.get_quote_token_balance())
                print('%s balance: %f' % (base_token, wallet[base_token]))
                print('target balance %f \n' % target[base_token])

                if wallet[base_token] < target[base_token]:
                    size = (target[base_token] - wallet[base_token]) * buy_scaling
                    market_pair = base_token + "/" + QUOTE_TOKEN
                    print('%s balance: %f' % (base_token, wallet[base_token]))
                    print('target balance %f' % target[base_token])
                    print('Buying %f %s' % (size, base_token))
                    try:
                        order_result = exchange.create_market_buy_order(market_pair, size)
                        print('BUY: Succesful %s order at: %f USDT' % (market_pair, size))
                        print(order_result)
                    except InvalidOrder as e:
                        print('BUY: Invalid %s order for size %f USDT' % (market_pair, size))
                        print(e)











