import argparse
import os
from binance_logger import CryptoMarket,DB

parser = argparse.ArgumentParser(description='define api connections')

parser.add_argument("--influx_org",type=str,default=os.environ.get("INFLUX_ORG"))
parser.add_argument("--influx_token",type=str,default=os.environ.get("INFLUX_TOKEN"))
parser.add_argument("--influx_url",type=str,default=os.environ.get("INFLUX_URL"))
parser.add_argument("--binance_api_key",type=str,default=os.environ.get("BINANCE_API_KEY"))
parser.add_argument("--binance_api_secret",type=str,default=os.environ.get("BINANCE_API_SECRET"))

args = parser.parse_args()

def handler(event,context):
    try:
        market = CryptoMarket(binance_api_key=args.binance_api_key,binance_api_secret=args.binance_api_secret)
        db = DB(org=args.influx_org,token=args.influx_token,url=args.influx_url)
        market.record_price_data(db,"crypto_market")
    except Exception as e:
        return str(e)
    return "OK"

