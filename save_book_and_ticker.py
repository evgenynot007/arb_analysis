import ccxt
import create_order_book
import create_ticker
import threading
import time 
import inspect
import asyncio
import db_helper

#cnx = db_helper.CreateConnection()
#cursor = cnx.cursor()

lock_thick = threading.Lock()
lock_books = threading.Lock()
data_b = []



def get_order_book(id_exchange, symbol):
    cnx = db_helper.CreateConnection()
    cursor = cnx.cursor()
    exchange = (getattr(ccxt, id_exchange))()
    data = []
    count = 0
    prev_book_time = 0
    cur_time = time.time()
    while True:
        
        try:
            order_book = exchange.fetchL2OrderBook(symbol)
        except Exception as e:
            print("couldn't fetch book from " + id_exchange + " " + str(e))
            break

        if prev_book_time != order_book['timestamp']:
            prev_book_time = order_book['timestamp']
            exchange_order_book = {(id_exchange, symbol):order_book}
            data.append(exchange_order_book)
            count += 1
            
            if time.time() - cur_time > 3:
                print("saving books for " + id_exchange)
   
                lock_books.acquire()
                db_helper.insert_multiple_order_books(data, cnx, cursor)
                lock_books.release()
                data = []
                print("done")
                cur_time = time.time()


def get_ticker(id_exchange, symbol):
    cnx = db_helper.CreateConnection()
    cursor = cnx.cursor()
    exchange = (getattr(ccxt, id_exchange))()
    data = []
    count = 0
    prev_ticker_time = 0
    cur_time = time.time()
    while True:
        try:
            ticker = exchange.fetchTicker(symbol)
        except Exception as e:
            print("couldn't fetch ticker from " + id_exchange + " " + str(e))
            break
        if prev_ticker_time != ticker['timestamp']:
            prev_ticker_time = ticker['timestamp']
            exchange_ticker = {(id_exchange, symbol):ticker}
            data.append(exchange_ticker)
            count += 1
            if time.time() - cur_time > 3:
                print("saving ticks for " + id_exchange)
                lock_thick.acquire()
                db_helper.insert_multiple_tickers(data, cnx, cursor)
                lock_thick.release()
                data = []
                print("done")
                cur_time = time.time()

def save_ticker(id_exchange, symbol):
    pass


def save_book_and_ticker(id_exchange, symbol):
    
    t1 = threading.Thread(target = get_order_book, args = (id_exchange, symbol))
    t2 = threading.Thread(target = get_ticker, args = (id_exchange, symbol))
    
    t1.start()
    t2.start()


def save_m(ls, symbol):
    
    for ex in ls:
        t = threading.Thread(target = save_book_and_ticker(ex, symbol))
        t.start()

save_book_and_ticker("exmo", "BTC/USD")
#get_ticker("coingi", "BTC/USD")

#save_m(["coinmarketcap", "cex"], "BTC/USD")
