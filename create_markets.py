
import db_helper
import ccxt 

cnx = db_helper.CreateConnection()
cursor = cnx.cursor()
#ccex ddos
name_exchange = ["acx", "binance", "bitfinex", "bitfinex2", "bithumb", "bitlish", "bittrex", 
            "bleutrade", "btcturk",  "cex", "coinexchange", 
            "coingi", "coinmarketcap", "exmo", "gatecoin", "gateio", "hitbtc",
            "hitbtc2", "kraken", "kucoin", "livecoin", "luno", "okex",
            "poloniex", "qryptos", "quoinex", "southxchange", "therock", "wex"]

def create_exchange_and_market_in_db():

    exchanges = {}

    for id in name_exchange:

        exchange = getattr(ccxt, id)
        exchanges[id] = exchange()

        id_exchage = db_helper.insert_exchage_to_db(exchanges[id],cnx,cursor)

        markets = exchanges[id].load_markets()

        for mark in markets:
            id_market = db_helper.insert_market_to_db( id_exchage, mark, cnx,cursor)


create_exchange_and_market_in_db()