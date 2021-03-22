from pydantic import BaseModel,validate_arguments
from binance.client import Client
from typing import Dict,List
import os
from time import time,time_ns
import multiprocessing
from .influx_logger import DB
from datetime import datetime

def create_binance_client(api_key=None,api_secret=None):
    if api_key is None:
        api_key = os.environ.get("BINANCE_API_KEY")
    if api_secret is None:
        api_secret = os.environ.get("BINANCE_API_SECRET")
    return Client(api_key, api_secret)


class Price(BaseModel):
    bidPrice:float
    bidQty:float
    askPrice:float
    askQty:float 

class Kline(BaseModel):
    openTime:int
    open:float
    high:float
    low:float
    close:float
    volume:float
    closeTime:int
    quoteAssetVolume:float
    numberOfTrades:int
    takerBuyBaseAssetVolume:float
    takerBuyQuoteAssetVolume:float
    Ignore:float

def get_pair_kline(pair_name):
    client = create_binance_client()
    k = client.get_klines(symbol=pair_name,interval=client.KLINE_INTERVAL_1MINUTE,limit=6)
    klines = [Kline.parse_obj(dict(zip(Kline.__fields__.keys(),i))) for i in k]
    return (pair_name,klines)



class Pair(BaseModel):
    symbol:str
    baseAsset:str
    quoteAsset:str
    price:Price=None
    kline:List[Kline]=None

    def get_last_recorded_kline_time(self,db):
        if self.kline is not None:
            query=f"""from(bucket: "crypto_market")
                |> range(start: -25m)
                |> filter(fn: (r) => r["_measurement"] == "kline")
                |> filter(fn: (r) => r["symbol"] == "{self.symbol}")
                |> last()"""
            query_result = db.query_api.query(query)
            times = []
            for table in query_result:
                for entry in table:
                    times.append(entry.get_time())
            if len(times)>0:
                return max(times)



class CryptoMarket:
    def __init__(self,binance_api_key=None,binance_api_secret=None,quote_asset:str="USDT",db:DB=None):
        if binance_api_key is not None:
            os.environ["BINANCE_API_KEY"] = binance_api_key
        if binance_api_secret is not None:
            os.environ["BINANCE_API_SECRET"] = binance_api_secret
        self.quote_asset = quote_asset
        self.client = create_binance_client(binance_api_key,binance_api_secret)
        self.update_pairs()
        self.update_pairs_prices()
        # self.update_pairs_klines()
    def update_pairs(self):
        exchange_info =  self.client.get_exchange_info()
        self.pairs = {}
        for s in exchange_info['symbols']:
            self.pairs[s['symbol']] = Pair.parse_obj(s)
        self.update_pairs_prices()
    def update_pairs_prices(self):
        tickers = self.client.get_orderbook_ticker()
        for ticker in tickers:
            symbol = ticker['symbol']
            if symbol in self.pairs:
                self.pairs[symbol].price = Price.parse_obj(ticker)
    def update_pairs_klines(self):
        kline_pair_symbols = [s for s in self.pairs if s.endswith(self.quote_asset)]
        pool = multiprocessing.Pool()
        output = pool.map(get_pair_kline,kline_pair_symbols)
        for pair_symbol,klines in output:
            self.pairs[pair_symbol].kline = klines 
    def record_price_data(self,db:DB,bucket_name):
        points = []
        for pair in self.pairs.values():
            pair_dict = pair.dict()
            tags = {key:val for key,val in pair_dict.items() if key not in ["price","kline"]}
            point = {
                "measurement":"price",
                "tags":tags,
                "fields":pair_dict['price'],
                "time":time_ns()
            }
            points.append(point)

        db._write_client.write(
            bucket_name,
            db.org,
            points
        )
    def record_kline_data(self,db,bucket_name):
        points = []
        for pair in self.pairs.values():
            if isinstance(pair.kline,list) and len(pair.kline)>0:
                last_time = pair.get_last_recorded_kline_time(db)
                pair_dict = pair.dict()
                tags = {key:val for key,val in pair_dict.items() if key not in ["price","kline"]}
                for k in pair.kline:
                    point_datetime = datetime.fromtimestamp(k.closeTime/1000)
                    if last_time is None or point_datetime.timestamp()>last_time.timestamp():
                        point = {
                            "measurement":"kline",
                            "tags":tags,
                            "fields":{key:val for key,val in k.dict().items() if key not in ["openTime","closeTime"]},
                            "time":k.closeTime
                        }
                        points.append(point)
        db._write_client.write(bucket_name,db.org,points,'ms')