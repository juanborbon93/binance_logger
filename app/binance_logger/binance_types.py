from pydantic import BaseModel,validate_arguments
from binance.client import Client
from typing import Dict,List
import os
from time import time
import multiprocessing
from .influx_logger import DB

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
    k = client.get_klines(symbol=pair_name,interval=client.KLINE_INTERVAL_1MINUTE,limit=1)
    return (pair_name,k)



class Pair(BaseModel):
    symbol:str
    baseAsset:str
    quoteAsset:str
    price:Price=None
    kline:Kline=None

    def update_kline(self,kline_data):
        updated = False
        for k in kline_data:
            kline_dict = dict(zip(Kline.__fields__.keys(),k))
            if self.kline is None:
                self.kline = Kline.parse_obj(kline_dict)
                updated=True
            if k[0]>self.kline.openTime:
                self.kline = Kline.parse_obj(kline_dict)
                updated=True
        return updated


class CryptoMarket:
    def __init__(self,binance_api_key=None,binance_api_secre=None,quote_asset:str="USDT"):
        self.quote_asset = quote_asset
        self.client = create_binance_client()
        t = time()
        self.update_pairs()
        print(f'took {time()-t} seconds to get pairs info')
        t = time()
        self.update_pairs_prices()
        print(f'took {time()-t} seconds to get price info')
        t = time()
        self.update_pairs_klines()
        print(f'took {time()-t} seconds to get kline info')
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
        outputs = pool.imap(get_pair_kline,kline_pair_symbols)
        for (pair_symbol,kline_data) in outputs:
            self.pairs[pair_symbol].update_kline(kline_data)