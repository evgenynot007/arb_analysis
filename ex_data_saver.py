
import asyncio
import os
import sys
import json
import time
import db_helper

root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(root + '/python')

import ccxt.async as ccxt  # noqa: E402
#ccex ddos
name_exchange = ["acx", "binance", "bitfinex", "bitfinex2", "bithumb", "bitlish", "bittrex", 
            "bleutrade", "btcturk",  "cex", "coinexchange", 
            "coingi", "coinmarketcap", "exmo", "gatecoin", "gateio", "hitbtc",
            "hitbtc2", "kraken", "kucoin", "livecoin", "luno", "okex",
            "poloniex", "qryptos", "quoinex", "southxchange", "therock", "wex"]


async def fetch_ticker(exchanges, name):
    item = exchanges[name]

    try:
        ticker = await item.fetchTickers()
       
        return {name:ticker}
    except Exception as e:
        faik = ""


def save():


    markets = db_helper.get_data_exch()

    loop = asyncio.get_event_loop()
    exchanges = {}

    for id in name_exchange:
        exchange = getattr(ccxt, id)
        #isHas = exchange.hasFetchTickers
        #if isHas:
        exchanges[id] = exchange({
            'enableRateLimit': True,  # or .enableRateLimit = True later
        })


    cnx = db_helper.CreateConnection()
    cursor = cnx.cursor()

    
    while True:
        start_time = time.time()
        input_coroutines = [fetch_ticker(exchanges, name) for name in exchanges]
        exch_tickers = loop.run_until_complete(asyncio.gather(*input_coroutines, return_exceptions=True))
        
        count_exchange = 0
        
        delta = time.time() - start_time
        
        for tickers in exch_tickers:
            if  tickers is not None:
                count_exchange+=1
        
        inserted_start = time.time()
        db_helper.insert_tick(markets,exch_tickers,cnx,cursor)
        inserted_time = time.time()
        print(count_exchange," ", delta, ' ', inserted_start - inserted_time)
    #loop.close()


save()

