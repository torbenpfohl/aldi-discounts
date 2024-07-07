"""
coordinates the market retrieval and the offer retrieval
knows/defines the database-paths for markets and offers.
"""
import os
from pathlib import Path
from datetime import date
from time import localtime
from datetime import timedelta

import discounts
import marketlists
from market import get_last_update
from market import store as store_markets

SOURCE_PATH = Path(__file__).resolve().parent
MARKET_DB_PATH = os.path.join(SOURCE_PATH, "markets.db")

class Markets:
  
  threshold = timedelta(days=15)

  @staticmethod
  def create_market_db():
    # TODO look into running asynchronously
    for market in ["Penny", "Aldi_nord"]:  # marketlists.__all__:  
      #TODO don't use rewe and aldi sued until progress bar and error handling are done.
      # check last update of @{market}
      market_type = getattr(marketlists, market).TYPE
      now = localtime()
      now_date = date(now.tm_year, now.tm_mon, now.tm_mday)
      last_update = get_last_update(market_type, MARKET_DB_PATH)
      if last_update + Markets.threshold <= now_date:
        markets = getattr(marketlists, market).get_markets()
        store_markets(markets, MARKET_DB_PATH)
        print(market, " - all markets stored.")
      else:
        print(market, "is up to date. last_update:", last_update)

class Products:
  pass

if __name__ == "__main__":
  Markets.create_market_db()