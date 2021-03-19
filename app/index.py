import argparse
import os
from binance_logger import CryptoMarket,DB

parser = argparse.ArgumentParser(description='define api connections')

parser.add_argument("influx_org",type=str,default=os.environ.get("INFLUX_ORG"))
parser.add_argument("influx_token",type=str,default=os.environ.get("INFLUX_TOKEN"))
parser.add_argument("influx_url",type=str,default=os.environ.get("INFLUX_URL"))
parser.add_argument("binance_api_key",type=str,default=os.environ.get("BINANCE_API_KEY"))
parser.add_argument("binance_api_secret",type=str,default=os.environ.get("BINANCE_API_SECRET"))

args = parser.parse_args()

