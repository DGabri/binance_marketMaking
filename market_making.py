from datetime import datetime
from urllib.parse import urljoin, urlencode
from decimal import Decimal
from typing import Union
import sqlalchemy as db
import pandas as pd
import websocket
import threading
import requests
import logging
import hashlib
import json
import time
import hmac
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(filename)s:%(lineno)s] %(message)s", datefmt="%d/%m/%Y %H:%M:%S")

API_KEY = "INSERT YOUR PUBLIC KEY"
SECRET_KEY = "INSERT YOUR PRIVATE KEY"
TARGET_SPREAD = float()

###################################################################################################
###################################################################################################

BASE_URL = "https://api.binance.com"
SPOT_STREAM_END_POINT = "wss://stream.binance.com:9443"
BINANCE_END_POINT = "https://api.binance.com/api/v3/userDataStream"

###################################################################################################
###################################################################################################
#DB INITIALIZATION
logging.info('INITIALIZING DB...')
result_engine = db.create_engine('sqlite:///mm_binance.db')
connection = result_engine.connect()
logging.info('DB INITIALIZED...')

#binance exceptions
class BinanceException(Exception):
    def __init__(self, status_code, data):

        self.status_code = status_code
        if data:
            self.code = data["code"]
            self.msg = data["msg"]
        else:
            self.code = None
            self.msg = None
        message = f"{status_code} [{self.code}] {self.msg}"

        # Python 2.x
        # super(BinanceException, self).__init__(message)
        super().__init__(message)

#signature payload
headers = {
    "X-MBX-APIKEY": API_KEY
}

##############################################################################################################################
def get_symbol_info(symbol):
    r = requests.get("https://api.binance.com/api/v3/exchangeInfo?symbol="+str(symbol.upper()))
    if r.status_code == 200:
        data = r.json()
        return data
    else:
        raise BinanceException(status_code=r.status_code, data=r.json())

def get_price_filter(symbol):
    info = get_symbol_info(symbol.upper())
    filter = info["symbols"][0]["filters"]
    return filter

def delete_order(symbol, order_id):
    PATH = "/api/v3/order"
    timestamp = int(time.time() * 1000)
    params = {
        "symbol": str(symbol),
        "orderId": int(order_id),
        "recvWindow": 5000,
        "timestamp": timestamp
    }

    query_string = urlencode(params)
    params["signature"] = hmac.new(SECRET_KEY.encode("utf-8"), query_string.encode("utf-8"), hashlib.sha256).hexdigest()

    url = urljoin(BASE_URL, PATH)
    r = requests.delete(url, headers=headers, params=params)
    if r.status_code == 200:
        data = r.json()
        return data
    else:
        raise BinanceException(status_code=r.status_code, data=r.json())


#create order
def create_order(symbol, side, mode, TIF, quantity, price):
    PATH = "/api/v3/order"
    timestamp = int(time.time() * 1000)
    params = {
        "symbol": str(symbol),
        "side": str(side),
        "type": str(mode),
        "timeInForce": str(TIF),
        "quantity": float(quantity),
        "price": float(price),
        "recvWindow": 5000,
        "timestamp": timestamp
    }
    query_string = urlencode(params)
    params["signature"] = hmac.new(SECRET_KEY.encode("utf-8"), query_string.encode("utf-8"), hashlib.sha256).hexdigest()
    url = urljoin(BASE_URL, PATH)
    r = requests.post(url, headers=headers, params=params)
    if r.status_code == 200:
        data = r.json()
        return data
        #return order_id
    else:
        return r.json()


def get_open_orders(symbol):
    PATH = "api/v3/openOrders"
    timestamp = int(time.time() * 1000)
    params = {
        "symbol": str(symbol),
        "recvWindow": 5000,
        "timestamp": timestamp
    }

    query_string = urlencode(params)
    params["signature"] = hmac.new(SECRET_KEY.encode("utf-8"), query_string.encode("utf-8"), hashlib.sha256).hexdigest()

    url = urljoin(BASE_URL, PATH)
    r = requests.get(url, headers=headers, params=params)
    if r.status_code == 200:
        data = r.json()
        return data
    else:
        raise BinanceException(status_code=r.status_code, data=r.json())

def check_order_fill(symbol):        
    a = get_open_orders(str(symbol.upper()))     
    if len(a) == 0:
        return 0
    else:
        return 1

def get_fees(symbol):
    PATH = "sapi/v1/asset/tradeFee"
    timestamp = int(time.time() * 1000)
    params = {
        "symbol":symbol,
        "recvWindow": 5000,
        "timestamp": timestamp
    }

    query_string = urlencode(params)
    params["signature"] = hmac.new(SECRET_KEY.encode("utf-8"), query_string.encode("utf-8"), hashlib.sha256).hexdigest()

    url = urljoin(BASE_URL, PATH)
    r = requests.get(url, headers=headers, params=params)
    if r.status_code == 200:
        data = r.json()
        return data
    else:
        raise BinanceException(status_code=r.status_code, data=r.json())


def get_account_info():
    PATH = "api/v3/account"
    timestamp = int(time.time() * 1000)
    params = {
        "recvWindow": 5000,
        "timestamp": timestamp
    }

    query_string = urlencode(params)
    params["signature"] = hmac.new(SECRET_KEY.encode("utf-8"), query_string.encode("utf-8"), hashlib.sha256).hexdigest()

    url = urljoin(BASE_URL, PATH)
    r = requests.get(url, headers=headers, params=params)
    if r.status_code == 200:
        data = r.json()
        return data
    else:
        raise BinanceException(status_code=r.status_code, data=r.json())


def get_free_coin(asset):
    res = get_account_info()
    if "balances" in res:
        for bal in res["balances"]:
            if bal["asset"].lower() == asset.lower():
                return bal#["free"]
    return None

def round_price(price, symbol):
    return round_step_size(price, float(position["COINS"][symbol]["price_tick"]))

#USED TO ROUND QUANTITY TO BYBIT"s REQUIREMENTS
def round_qty(qty, symbol):
    return round_step_size(qty, float(position["COINS"][symbol]["size_step"]))

#LA USAVO PER BINANCE, QUELLE SOPRA DOVREBBERO ANDARE BENE
def round_step_size(quantity: Union[float, Decimal], step_size: Union[float, Decimal]) -> float:
    """Rounds a given quantity to a specific step size
    :param quantity: required
    :param step_size: required
    :return: decimal
    """
    quantity = Decimal(str(quantity))
    return float(quantity - quantity % Decimal(str(step_size)))

def trade_time(buy_time):
    diff = ((time.time()*1000)-buy_time)
    return diff/1000

###################################################################################################
###################################################################################################



position_val = 0
buy_qty = 0
buy_price = 0
position_open_time = 0
take_profit = 0
order_id = 0
order_filled = 0
executed = 0
busd = float(get_free_coin("BUSD")["free"])*0.9

#position_val = check_order_fill("ETHBUSD")

if (busd < 10):# and (position_val == 0):
    sys.exit("INSUFFICIENT BALANCE")

lock = threading.Lock()

def update_executed(bought_price):
    global executed, buy_price
    buy_price = float(bought_price)
    executed = 1

def position_opened(bought_qty):
    global position_val, buy_qty, position_open_time, order_filled, buy_price
    with lock:
        order_filled = 1
        position_val = 1
        buy_qty = bought_qty
        position_open_time = time.time()


def position_closed():
    global position_val, buy_qty, position_open_time, take_profit, order_id, order_filled, buy_price, executed
    with lock:
        buy_price = 0
        executed = 0
        order_filled = 0
        order_id = 0
        position_val = 0
        buy_qty = 0
        take_profit = 0 
        position_open_time = 0

def order_cancelled(coin_name, orderId):
    global position_val, buy_qty, position_open_time, take_profit, order_id, order_filled, buy_price, executed
    if order_id != '':
        delete_order(coin_name, orderId)
        with lock:
            executed = 0
            buy_price = 0
            order_filled = 0
            order_id = 0
            take_profit = 0
            position_val = 0
            buy_qty = 0
            position_open_time = 0

def update_busd(new_qty):
    global busd
    with lock:
        busd = new_qty

def update_order_id(id):
    global order_id
    with lock:
        order_id = id

def update_take_profit(tp_val):
    global take_profit
    with lock:
        take_profit = float(tp_val)

###################################################################################################
###################################################################################################

def create_spot_listen_key(api_key):
    response = requests.post(url=BINANCE_END_POINT, headers={"X-MBX-APIKEY": api_key})
    return response.json()["listenKey"]

listen_key = create_spot_listen_key(API_KEY)

logging.info("CREATED LISTEN KEY...")
url = f"{SPOT_STREAM_END_POINT}/ws/{listen_key}"
logging.info("URL: " + url)

###################################################################################################

#target_spread = float(input("Set desired profit(spread) per trade: "))
#SPREAD ALTI btceur, btctusd, 
position = {"OPEN_POSITIONS":0, "COINS":{}}
coin_name = "ETHBUSD"
target_spread = 0.01
trade_data_dict = {"buy_time":0, "buy_price":0, "sell_price":0,
 "qty":0, "position":0, "tp":0, "orderId":0}

position["COINS"][coin_name] = {"buy_price":0, "buy_fees":0, "buy_qty":0, "tp":0,
 "sl":0, "price_tick":0, "size_step":0, "maker_fees":0, "buy_time":0}


size = get_price_filter(coin_name)    
position["COINS"][coin_name]["size_step"] = size[2]["stepSize"]
position["COINS"][coin_name]["price_tick"] = size[0]["tickSize"]


def ws_order_book(symbol): 
    socket = f"wss://stream.binance.com/ws/{symbol.lower()}@bookTicker"
    
    def ob_on_message(wsapp, message): 
        global trade_data_dict, busd, position_val, buy_qty, position_open_time, take_profit, order_id, order_filled, executed
    
        json_message = json.loads(message)
        #logging.info(json_message)
        ask = float("{0:.6f}".format(float(json_message["a"])))
        bid = float("{0:.6f}".format(float(json_message["b"])))
        #ask_qty = float("{0:.5f}".format(float(json_message["A"])))
        #bid_qty = float("{0:.5f}".format(float(json_message["B"])))
        tick_size = float(position["COINS"][coin_name]["price_tick"])
        #step_size = float("{0:.8f}".format(lot_step_size))
        my_bid = float("{0:.6f}".format(float(bid+tick_size)))
        my_ask = float("{0:.6f}".format(float(ask-tick_size)))
        #my_ask = float("{0:.6f}".format(float(my_bid + (2*tick_size))))
        my_gain = float("{0:.6f}".format(float(((my_ask-my_bid)/my_ask)*100)))
        
        #calculate spread
        #spread = float("{0:.6f}".format(float(((ask-bid)/ask)*100)))
        
        initial_qty = busd
        """logging.info(f"position: {position_val}")
        logging.info(f"buy_qty: {buy_qty}")
        logging.info(f"open_time: {position_open_time}")
        logging.info(f"busd_qty: {busd}")"""

        now = time.time()
        timestamp = datetime.now()

        if ((timestamp.hour < 20) and (timestamp.hour > 2)):
            #(order_filled == 0) and 
            if ((position_val == 1) and (executed == 0) and ((now-position_open_time) > 10)):
                #logging.info("DELETE ORDER REQUEST")
                #logging.info(f"ORDER_ID: {order_id}")
                cancel_order = order_cancelled(coin_name, trade_data_dict["orderId"])
                logging.info(cancel_order)
                if len(cancel_order.keys()) != 0:
                    if 'code' in cancel_order:
                        update_order_id('')

                #logging.info(f"UPDATED ORDER_ID: {order_id}")
                #logging.info("============================")
            
            #and (my_gain > TARGET_SPREAD)
            if ((position_val == 0) and (my_gain > TARGET_SPREAD)):
                #logging.info(f"SPREAD: {my_gain}")
                #SEND ORDER
                eth_qty = (initial_qty/my_bid)
                buy_qty = round_qty(eth_qty, coin_name)

                #"fills": [{"price": "1657.73000000", "qty": "0.00790000", "commission": "0.00000000", "commissionAsset": "BNB", "tradeId": 366417212}]}

                #order = create_order(coin_name, "buy", "limit", "IOC",  buy_qty, my_bid)
                order = create_order(coin_name, "buy", "limit", "GTC",  buy_qty, my_bid)
                #logging.info(order)
                #logging.info(order.keys())
                trade_data_dict["orderId"] = order["orderId"]
            
                #update_order_id(trade_data_dict["orderId"])
                update_take_profit(float(my_ask))
                position_opened(buy_qty)
                        

                #logging.info("============================")
                
    def ob_on_error(wsapp,error):
        logging.info("ORDERBOOK ERROR...")
        logging.info(error)
        logging.info("------------------")

    def ob_on_open(ws):
        logging.info("opened connection to Binance streams")

    def ob_on_close(ws):
        logging.info("closed connection to Binance streams") 

    wsapp = websocket.WebSocketApp(
            socket,
            on_message=ob_on_message, 
            on_open=ob_on_open, 
            on_close=ob_on_close, 
            on_error=ob_on_error)
            
    wsapp.run_forever()

###################################################################################################
###################################################################################################


def position_on_open(ws):
    print(f"Open: spot order stream connected")
    print(f"URL: {url}")

def position_on_message(ws, message):
    global take_profit, buy_qty, buy_price, executed
    event_dict = json.loads(message)
    
    if event_dict["e"] == "executionReport":  # event type
        #logging.info(event_dict)
        order_status = event_dict["x"]
        if order_status == "TRADE":  # order fill
            ts = event_dict["E"]
            symbol = event_dict["s"]
            id = event_dict["i"]  # int
            price = float(event_dict["p"])  # float as string
            side = event_dict["S"]  # "BUY" or "SELL"
            qty = float(event_dict["q"])  # float as string
            fee = float(event_dict["n"])  # float as string
            

            if side == "BUY":
                    
                profit = 0

                #time.sleep(2)
                eth = float(get_free_coin("ETH")['free'])*0.98
                #logging.info(f"free ETH: {eth}")
                #logging.info(f"ETH to sell: {buy_qty}")
                update_executed(price)
                order = create_order(coin_name, "sell", "limit", "GTC", round_qty(eth, "ETHBUSD"), take_profit)
                #logging.info(order)

                if 'code' in order:
                    logging.info("SLEEPING...")
                    time.sleep(2)
                    eth = float(get_free_coin("ETH")['free'])*0.98
                    #logging.info(f"free ETH: {eth}")
                    #new_order = create_order(coin_name, "sell", "limit", "GTC", buy_qty, take_profit)
                    new_order = create_order(coin_name, "sell", "limit", "GTC", round_qty(eth, "ETHBUSD"), take_profit)
                    
                    if "code" in new_order:
                        
                        logging.info(new_order)
                    logging.info("FINISHED SLEEPING")
                    #logging.info("***************")

                msg = {"time":ts, "profit":profit, "base_vol":qty, "usdt_vol":qty*price, "eth":eth, "busd":float(get_free_coin("BUSD")['free']), "duration":0}
            else:

                #check if size is 0                
                duration = trade_time(int(position["COINS"][symbol]["buy_time"]))
                #payload = get_open_orders(coin_name)
                #logging.info(payload)
                #NO OPEN ORDERS
   
                profit = (((price-buy_price)/buy_price)*100)-(position["COINS"][symbol]["buy_fees"])
                position_closed()

                msg = {"time":ts, "profit":profit, "base_vol":qty, "usdt_vol":qty*price, "eth":float(get_free_coin("ETH")['free']), "busd":float(get_free_coin("BUSD")['free']), "duration":duration}
            logging.info(f"{symbol}: {side} {qty} at {price}; fee {fee}; profit {profit}")

            #SAVE RESULT TO DB
            
            frame = pd.DataFrame([msg])
            frame.to_sql("trades", result_engine, if_exists="append", index=False)
            #logging.info(msg)
            

def position_on_error(ws, error):
    logging.info(f"Position Error: {error}")
    logging.info("+++++++++++++++++++++++")

def position_on_close(ws, close_status_code, close_msg):
    print(f"Close: {close_status_code} {close_msg}")

def stream_ticker():
    ws = websocket.WebSocketApp(url=url,
                                on_open=position_on_open,
                                on_message=position_on_message,
                                on_error=position_on_error,
                                on_close=position_on_close)
    ws.run_forever(ping_interval=300)


###################################################################################################
###################################################################################################



orderbook = threading.Thread(target=ws_order_book, args=(coin_name, ))
position_stream = threading.Thread(target=stream_ticker)
orderbook.start()
position_stream.start()
