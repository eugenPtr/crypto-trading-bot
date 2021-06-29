from binance import Client
from config import TEST_API_KEY, TEST_API_SECRET
from executioner import Executioner
import pandas as pd
import bisnita
import time

if __name__ == '__main__':

    client = Client(TEST_API_KEY, TEST_API_SECRET, testnet=True)
    executioner = Executioner(client, 0)

    dataset = executioner.build_and_save_new_dataset()
    print(dataset)

    spans = [20, 50, 100]
    model = bisnita.BișnițăModel(spans, klines_per_day=60 * 24)
    weights = model.mama_omida(dataset)

    while True:
        time.sleep(60)
        dataset = executioner.build_and_save_new_dataset()
        print(dataset)
        if dataset.empty == False:
            current_traded_tokens_balance = executioner.get_traded_tokens_balance()
            sell_token_balance = executioner.get_sell_token_balance()

            weights = model.mama_omida(dataset)

            portfolio = pd.Series(current_traded_tokens_balance)

            prices = dataset['Close'].iloc[-1]
            target = bisnita.următoarea_combinație(weights.iloc[-1], portfolio, sell_token_balance, prices)
            print(target)

            # TODO: Execute market orders
            # order = client.order_market_sell(
            #     symbol='BNBBTC',
            #     quantity=100)





