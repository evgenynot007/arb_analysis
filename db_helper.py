
import mysql.connector
import time
from operator import itemgetter
import numpy as np

#from mysql.connector import errorcode


def CreateConnection():

    config = {
      'user': '',
      'password': '',
      'host': 'localhost',
      'database': '',
      'raise_on_warnings': True,
    }

    cnx = mysql.connector.connect(**config)

    return cnx


def change_to_timestamp(date_str):
   # date_str = "2008-11-10 17:53:59"
    timestamp = time.mktime(time.strptime(date_str, "%Y-%m-%d %H:%M:%S"))
    return timestamp




def insert_exchage_to_db(exchage,cnx,cursor):

    add_exchage = ("INSERT INTO exchange "
              "(name) "
              "VALUES (%(name)s)")

    data_exchage = {
        'name': exchage.id
    }

    cursor.execute(add_exchage, data_exchage)
    cnx.commit()
    return cursor.lastrowid



def insert_market_to_db( id_exchage, mark, cnx,cursor):

    add_m = ("INSERT INTO market "
              "(name, id_exchange) "
              "VALUES (%(name)s, %(id_exchange)s)")

    data_m = {
        'name': mark,
        'id_exchange': id_exchage
    }

    cursor.execute(add_m, data_m)
    cnx.commit()
    return cursor.lastrowid

def get_data_exch():

    cnx = CreateConnection()
    cursor = cnx.cursor()
    query = ("SELECT mark.id as market_id, ex.name as ex_name, mark.name as mark_name FROM exchange ex LEFT JOIN market mark ON mark.id_exchange = ex.id WHERE mark.id IS NOT NULL;")

   
    cursor.execute(query)
    markets = {}
    for (market_id, ex_name, mark_name) in cursor:
        markets[ex_name + "_" + mark_name] = market_id

    cursor.close()
    cnx.close()

    return markets


    
def insert_fake_deal(data):
    
    cnx = CreateConnection()
    cursor = cnx.cursor()

    add_fake_deal = ("INSERT INTO fake_deals (price, volume, time_opened, time_closed, gain, spread, bid, id_exchange) VALUES(%(price)s, %(volume)s, %(time_opened)s, %(time_closed)s, \
                                %(gain)s, %(spread)s, %(bid)s, %(market_id)s)")
    
    cursor.execute(add_fake_deal, data)
    cnx.commit()

    return cursor.lastrowid

def insert_angle_price_deal(data):
    cnx = CreateConnection()
    cursor = cnx.cursor()
    
    add_angle_price_deal = ("INSERT INTO angle_price_deal (id_deal, 1_sec, 5_sec, 10_sec, 30_sec, 1_min, 3_min, 10_min, id_exchange)\
                            VALUES(%(id_deal)s, %(1_sec)s, %(5_sec)s, %(10_sec)s, %(30_sec)s, %(1_min)s, %(3_min)s, %(10_min)s, %(id_exchange)s)")
    
    cursor.executemany(add_angle_price_deal, data)
    cnx.commit()


def insert_angle_volume_deal(data):

    cnx = CreateConnection()
    cursor = cnx.cursor()
    print(data)
    add_angle_volume_deal = ("INSERT INTO angle_volume_deal (id_deal, 1_sec, 5_sec, 10_sec, 30_sec, 1_min, 3_min, 10_min, id_exchange)\
                            VALUES(%(id_deal)s, %(1_sec)s, %(5_sec)s, %(10_sec)s, %(30_sec)s, %(1_min)s, %(3_min)s, %(10_min)s, %(id_exchange)s)")
    
    cursor.executemany(add_angle_volume_deal, data)
    cnx.commit()

def insert_order_deal(data):

    cnx = CreateConnection()
    cursor = cnx.cursor()

    try:

        add_order_deal = ("INSERT INTO order_deal (id_deal, id_order)\
                            VALUES(%(id_deal)s, %(id_order)s)")
    
        cursor.execute(add_order_deal, data)
        cnx.commit()
    except:
        print("sad3")

    return cursor.lastrowid

def insert_multiple_order_books(markets, data, cnx, cursor):
    if not data:
        return

    stmt = "INSERT INTO order_books (id_market, local_time, timestamp, price, amount, bid) VALUES (%s, %s, %s, %s, %s, %s)"
    books = []
    curr_time = time.time();
    #markets = get_data_exch()
   # f = open('log_b', 'a')
    for exch_order_book in data:
        if exch_order_book is None:
            continue
        id_exchange = exch_order_book[0]
        symbol = exch_order_book[1]
        order_book = exch_order_book[2]
        timestamp = order_book['timestamp']
        id_market = markets[id_exchange + '_' + symbol]
        

        item = order_book['bids'][0]
        books.append((id_market, curr_time, timestamp, item[0], item[1], 1))
           
        item = order_book['asks'][0]
        books.append((id_market, curr_time, timestamp, item[0], item[1], 0))
        #f.write(str(timestamp) + '\n')
        
    cursor.executemany(stmt, books)
    cnx.commit()
            


#array_of_data contains tickers and books for each available exchange at each mls
def get_books_from_db(array_of_data, start_time, end_time):
    cnx = CreateConnection()
    cursor = cnx.cursor()
    query = ("SELECT id_market, timestamp, price, amount, bid FROM david.order_books WHERE timestamp >=  " + str(start_time) + " AND timestamp <= " + str(end_time))
    cursor.execute(query)
    rows = cursor.fetchall()
    
    print(len(rows))

    for item in rows:
        id_market = item[0]
        timestamp = item[1]
        price = item[2]
        amount = item[3]
        bid = item[4]
        key = 'bids' if bid else 'asks' #scpecifies key for dictionary that contains data 
        key_2 = 'asks' if bid else 'bids'
        if timestamp not in array_of_data:
            array_of_data.update({timestamp: 
                                    {id_market: 
                                        {'book': 
                                            {key: [
                                                    [price, amount]
                                                    ],
                                             key_2:[
                                                    ]}}}})  

        elif id_market not in array_of_data[timestamp]:
            array_of_data[timestamp].update({id_market: 
                                                {'book': 
                                                    {key: [
                                                        [price, amount]
                                                          ],
                                                     key_2:[
                                                        ]}}})
        elif 'book' not in array_of_data[timestamp][id_market]:
            array_of_data[timestamp][id_market].update({'book': {key: [ [price, amount] ], key_2: [] }})
        else: array_of_data[timestamp][id_market]['book'][key].append([price, amount])  #here problem might occur if different books have the same timestamp
        #print('done books ' + str(timestamp))
    return array_of_data

def insert_multiple_tickers(markets,data, cnx, cursor):
    if not data:
        return

    tickers = []
    stmt = "INSERT INTO exch_tick (id_market, local_time, timestamp, last, low, high, bid, ask, volume) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)" 
   # markets = get_data_exch()
    local_time = time.time()
   # f = open('log_t', 'w')
    for exch_ticker in data:
        if exch_ticker is None:
            continue
        id_exchange = exch_ticker[0]
        symbol = exch_ticker[1]
        tick = exch_ticker[2]
        id_market = markets[id_exchange + '_' + symbol]
        timestamp = tick["timestamp"]
        last = tick["last"]
        low = tick["low"]
        high = tick["high"]
        bid = tick["bid"]
        ask = tick["ask"]
        last = tick["last"]
        volume = tick["baseVolume"]
        tickers.append((id_market, local_time, timestamp, last, low, high, bid, ask, volume))
        #f.write(str(timestamp) + '\n')
    cursor.executemany(stmt, tickers)
    cnx.commit()

 #array_of_data contains tickers and books for each available exchange at each mls
def get_tickers_from_db(cnx, cursor, array_of_data, start_time, end_time, market_name): 
    query = ("SELECT id_market, timestamp, last, volume FROM david.exch_tick WHERE id_market\
            IN (SELECT id FROM david.market where market.name = '" + market_name + "') and timestamp >=  " + str(start_time) + " AND timestamp <= " + str(end_time)+" ORDER BY timestamp")
    cursor.execute(query)
    rows = cursor.fetchall()
    
    for item in rows:
        id_market = item[0]
        timestamp = item[1]
        last = item[2]
        volume = item[3]
        if timestamp not in array_of_data:
            array_of_data.update({timestamp: 
                                    {id_market: 
                                        {'tick': [last, volume]}}})  

        elif id_market not in array_of_data[timestamp]:
            array_of_data[timestamp].update({id_market:
                                                   {'tick': [last, volume]}})

        else: array_of_data[timestamp][id_market].update({'tick': [last, volume]})
        #print('done ticks ' + str(timestamp))
    return array_of_data


def insert_ohlcv(exch_ohlcv, cnx, cursor):
    stmt = "INSERT INTO candlestick (timestamp, id_exchange, symbol, Open, Hight, Lowest, Close, Volume) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"

    for exch in exch_ohlcv:
        id_exchange = exch
        symbol = exch_ohlcv[exch][0]
        for data in exch_ohlcv[exch][1]:
            timestamp = data[0]
            open = data[1]
            hight = data[2]
            lowest = data[3]
            close = data[4]
            volume = data[5]
            exch_data = (timestamp, id_exchange, symbol, open, hight, lowest, close, volume)
            
            cursor.execute(stmt, exch_data)
            cnx.commit
    
    cursor.close()
    cnx.close()


def get_marketsplot_by_name(market_name, start, end):
    print("dict where keys market_id and values are list of tuples from timestamp, last, exchange_id")
    cnx = CreateConnection()
    cursor = cnx.cursor()
  
    #args = (market_name, start, end)
    query = ("SELECT market.id as market_id, exch_tick.timestamp as timestamp, market.id_exchange as exchange_id, \
            exch_tick.last as last, exch_tick.bid, exch_tick.ask FROM  exch_tick INNER JOIN market on exch_tick.id_market=market.id where exch_tick.id_market\
            IN (SELECT id FROM david.market where market.name = '" + market_name + "') \
             and exch_tick.timestamp>=" + str(start) + " and exch_tick.timestamp<=" + str(end) + ";");
  
  
    markets_pair = dict()
    cursor.execute(query)
    data = cursor.fetchall()
    for (market_id, timestamp, exchange_id, last,bid,ask) in data:
        if market_id in markets_pair.keys():
            markets_pair[market_id].append((timestamp, last, exchange_id,bid,ask))
        else:
            markets_pair[market_id] = [(timestamp, last,exchange_id,bid,ask)]
    return markets_pair

def get_marketsplot_by_exchange_id(ex_id):
    cnx = CreateConnection()
    cursor = cnx.cursor()
    query = ("SELECT market.id as market_id, ticker.timestamp as timestamp, market.id_exchange as exchange_id, ticker.last as last  FROM \
            ticker INNER JOIN market on ticker.id_market=market.id where market.id_exchange="+str(ex_id)+";")
    #markets = list()
    
    #temp1 = []
    #temp2 = []
    markets_pair = dict()
    cursor.execute(query)
    for (market_id, timestamp, exchange_id, last) in cursor:
        #print(market_id,exchange_id)
        if market_id in markets_pair.keys():
            markets_pair[market_id].append((timestamp, last))
        else:
            markets_pair[market_id] = [(timestamp, last)]
            #temp2.append(float(last))
            #temp1.append(float(timestamp))
    #markets = list(zip(temp1, temp2))
    #print(markets_pair[188])
    #print(markets_pair)
    return markets_pair


def get_marketplot_between(market_name, start, end):
    cnx = CreateConnection()
    cursor = cnx.cursor()
    #print('start2')
    query = ("SELECT market.id as market_id, exch_tick.timestamp as timestamp, market.id_exchange as exchange_id, \
            exch_tick.last as last FROM exch_tick INNER JOIN market on exch_tick.id_market=market.id where \
            exch_tick.id_market IN (SELECT id FROM david.market where market.name = '" + market_name + "') and\
           exch_tick.timestamp>=" + str(start) + " and exch_tick.timestamp<=" + str(end) + ";");
    
    cursor.execute(query)
    #print('end2')


    return list(cursor)

def get_markets_list():
    cnx = CreateConnection()
    cursor = cnx.cursor()
    query = ("SELECT DISTINCT name FROM market limit 10000;")

    cursor.execute(query)
    markets_with_size = list()
    markets_list = list(cursor)
    markets_names = [(name) for (name) in markets_list]
    for name in markets_names:
        qr = ("SELECT COUNT(*) FROM market where market.name='"+name[0]+"';")
        cursor.execute(qr)
        for i in cursor:
            markets_with_size.append((name[0],i[0]))

    biggest_markets = sorted(markets_with_size, key=itemgetter(1))
    biggest_markets = [(a) for (a,b) in biggest_markets if b > 2]
    return biggest_markets
   

def insert_tick(markets_id,exch_tickers,cnx,cursor):
    curr_time = time.time()
    stmt = "INSERT INTO ticker (id_market, local_time,timestamp,last,low,high,bid,ask,volume) VALUES (%s, %s,%s, %s,%s, %s,%s, %s, %s)"
    
    for exch  in exch_tickers:
        if  exch is not None:
            for exch_name  in exch:
                for ticker  in exch[exch_name].items():
                    if exch_name + "_" + ticker[0] in markets_id:
                        id = markets_id[exch_name + "_" + ticker[0]]
                        tick = ticker[1]
                        timestamp = tick["timestamp"]
                        last = tick["last"]
                        low = tick["low"]
                        high = tick["high"]
                        bid = tick["bid"]
                        ask = tick["ask"]
                        volume = tick["baseVolume"]
                        if last is not None and volume is not None and volume > 0 :
                            data = []
                            data.insert(id, (id, curr_time, tick["timestamp"]/1000, tick["last"], tick["low"], tick["high"], tick["bid"], tick["ask"], volume))
                            cursor.executemany(stmt, data)
    

    cnx.commit()



def get_book_order(list_mrk_id, start, end):
    print("connect")
    cnx = CreateConnection()
    cursor = cnx.cursor()
    print(list_mrk_id)
    query = ("SELECT id_market, timestamp, price, amount, bid FROM david.order_books where timestamp >=" + str(start) + " and timestamp <=" + str(end) + ";")
    cursor.execute(query)

    time_and_order_book = dict()

    for i in list_mrk_id:
        time_and_order_book[i] = dict()


    data = cursor.fetchall()
    for (id_market, timestamp, price, amount, bid) in data:
        if id_market in time_and_order_book.keys():             #пофиксть дял одной из бирж
            if timestamp in time_and_order_book[id_market].keys():
                #print("=(", end='')
                time_and_order_book[id_market][timestamp].append((price, amount, bid))
            else:
                #print("=)", end='')
                time_and_order_book[id_market][timestamp] = [(price, amount, bid)]

    return time_and_order_book

def find_flat_exchanges( start, end, unic_prices):
    print("get flat exchang")
    cnx = CreateConnection()
    cursor = cnx.cursor()
  
    #args = (market_name, start, end)
    query = ("SELECT count(DISTINCT ticker.last) as unic_prices,  market.name as per_name, exchange.name as ex_name, market.id as market_id\
            FROM ticker INNER JOIN market on ticker.id_market=market.id \
            INNER JOIN exchange on exchange.id=market.id_exchange  where  ticker.timestamp>=" + str(start) +
           " and ticker.timestamp<=" + str(end) + "\
             GROUP by  market.name, market.id HAVING unic_prices >"+ str(unic_prices) +";");
  
 
    cursor.execute(query)
    unic_prices = cursor.fetchall()

    return unic_prices
 

def get_market_name():

    cnx = CreateConnection()
    cursor = cnx.cursor()
    query = ("SELECT  market.name as mark_name FROM  market ")

   
    cursor.execute(query)
    markets = {}
    for (mark_name) in cursor:
        markets[mark_name[0]] = mark_name[0]

    cursor.close()
    cnx.close()

    return markets
