"""
coordinates the market retrieval and the offer retrieval
knows/defines the database-paths for markets and offers.
"""
import os
import json
import logging
import sqlite3
from random import sample
from shutil import copyfile
from pathlib import Path
from datetime import date
from time import localtime
from datetime import timedelta
#from rich import progress

import discounts
import marketlists
from util import delay_range, set_week_start_and_end
from market import store as store_markets
from market import delete as delete_markets
from market import get_last_update, get_market_ids
from market_extra import store as store_market_extra_infos
from product import store as store_products
from product import products_present
from market_products import store_multiple as store_markets_with_product_ids
from group_id_markets import get_group_ids
from group_id_markets import store as store_selling_regions_with_markets

LOG_PATH_FOLDERNAME = "log"
TMP_PATH_FOLDERNAME = "tmp"
DATA_PATH_FOLDERNAME = "data"
BACKUP_PATH_FOLDERNAME = "backup"

SOURCE_PATH = Path(__file__).resolve().parent
if not (os.path.exists(TMP_PATH := os.path.join(SOURCE_PATH, TMP_PATH_FOLDERNAME))):
  os.mkdir(TMP_PATH)
if not (os.path.exists(DATA_PATH := os.path.join(SOURCE_PATH, DATA_PATH_FOLDERNAME))):
  os.mkdir(DATA_PATH)
if not (os.path.exists(LOG_PATH := os.path.join(SOURCE_PATH, LOG_PATH_FOLDERNAME))):
  os.mkdir(LOG_PATH)
if not (os.path.exists(BACKUP_PATH := os.path.join(SOURCE_PATH, BACKUP_PATH_FOLDERNAME))):
  os.mkdir(BACKUP_PATH)

MARKET_DB_PATH = os.path.join(DATA_PATH, "markets.db")
MARKET_EXTRA_DB_PATH = os.path.join(DATA_PATH, "market_extra.db")
PRODUCT_DB_PATH = os.path.join(DATA_PATH, "products.db")
MARKET_PRODUCTS_DB_PATH = os.path.join(DATA_PATH, "market_products.db")
SELLING_REGION_MARKETS_DB_PATH = os.path.join(DATA_PATH, "selling_region_markets.db")
REWE_MARKETLIST_TEMP_ZIPCODE_RANGE = os.path.join(TMP_PATH, "rewe_marketlist_temp.json")
REWE_DISCOUNTS_TEMP_MARKETS_TO_DO = os.path.join(TMP_PATH, "rewe_discount_temp.json")
NETTO_DISCOUNTS_TEMP_MARKETS_TO_DO = os.path.join(TMP_PATH, "netto_discount_temp.json")

# TODO: use Market retrieval with Threads 
# TODO: make delay more dynamic, ie. if we get an unexpected status_code or request error
#       we increase the delay and than lower it gradually again.
#       e.g. base=(50,500), min_factor=1 -> every problem set factor*2 -> lower factor every working request factor/2

LOG_LEVEL = "INFO"
logger = logging.getLogger(__name__)
log_handler = logging.FileHandler(os.path.join(LOG_PATH, f"{__name__}.log"), mode="w", encoding="utf-8")
log_formatter = logging.Formatter("%(name)s %(funcName)s (%(levelname)s) - %(asctime)s - %(message)s")
log_handler.setFormatter(log_formatter)
logger.addHandler(log_handler)
logger.setLevel(LOG_LEVEL)

logging.getLogger("marketlists").setLevel(LOG_LEVEL)

# TODO: save the failed requests (from rewe) and repeat them a bit later

class Markets:

  @staticmethod
  def create_market_db(market_types: list[str] = marketlists.__all__, force: bool | list[str] = False, backup: bool = True, threshold_market_renewal: int = 30):
    """Creates a table with only the newest markets.
    
    market_types: only create/run marketlist for those markets. (default is all available markets) @{marketlists.__all__}
    force: True = deletes all market-rows  |  list with the market_types to delete
    backup: Backup all databases (everything in the data-folder) and current rewe zip-range if exists
    threshold_market_renewal: threshold < 0 == force=True
    """
    if backup:
      now = localtime()
      now_date = "-".join([str(now.tm_year), str(now.tm_mon), str(now.tm_mday)])
      now_time = "-".join([str(now.tm_hour), str(now.tm_min), str(now.tm_sec)])
      # save tmp .json
      if os.path.exists(REWE_MARKETLIST_TEMP_ZIPCODE_RANGE):
        logger.warning("Rewe market isn't fully processed yet (%s)", REWE_MARKETLIST_TEMP_ZIPCODE_RANGE)
        current_filepath = REWE_MARKETLIST_TEMP_ZIPCODE_RANGE
        filename = REWE_MARKETLIST_TEMP_ZIPCODE_RANGE.split(os.sep)[-1]
        backup_filename = now_date + "_" + now_time + "_" + filename
        backup_filepath = os.path.join(BACKUP_PATH, backup_filename)
        while os.path.exists(backup_filepath):
          now = localtime()
          now_date = "-".join([str(now.tm_year), str(now.tm_mon), str(now.tm_mday)])
          now_time = "-".join([str(now.tm_hour), str(now.tm_min), str(now.tm_sec)])
          backup_filename = now_date + "_" + now_time + "_" + filename
          backup_filepath = os.path.join(BACKUP_PATH, backup_filename)
        copyfile(current_filepath, backup_filepath)
        logger.info("Saved %s - %s to %s", filename, current_filepath, backup_filepath)
      # save all databases
      for filename in os.listdir(DATA_PATH):
        if os.path.isdir(os.path.join(DATA_PATH, filename)):
          continue
        if filename.endswith(".db"):
          backup_filename = now_date + "_" + now_time + "_" + filename
          current_filepath = os.path.join(DATA_PATH, filename)
          backup_filepath = os.path.join(BACKUP_PATH, backup_filename)
          while os.path.exists(backup_filepath):
            now = localtime()
            now_date = "-".join([str(now.tm_year), str(now.tm_mon), str(now.tm_mday)])
            now_time = "-".join([str(now.tm_hour), str(now.tm_min), str(now.tm_sec)])
            backup_filename = now_date + "_" + now_time + "_" + filename
            backup_filepath = os.path.join(BACKUP_PATH, backup_filename)
          copyfile(current_filepath, backup_filepath)
          logger.info("Saved %s - %s to %s", filename, current_filepath, backup_filepath)
      logger.info("Backup done!")

    # Check and set the oldest allowed market. If exceeded = start the market run again. -> market types are checked individually
    # As well as checking for the delete all rows flag 'force'
    if not isinstance(threshold_market_renewal, int):
      logger.warning("Threshold market renewal value %s not an integer. Will be ignored.", threshold_market_renewal)
      threshold_market_renewal = 30
    elif threshold_market_renewal < 0:
      force = True
    if isinstance(force, bool) and force:
      logger.info("Delete all market-rows.")
      delete_markets(MARKET_DB_PATH)
    threshold_market_renewal = timedelta(days=threshold_market_renewal)

    # Was a market-list supplied?
    checked_market_types = list()
    for mt in market_types:
      if mt in marketlists.__all__:
        checked_market_types.append(mt)
      else:
        logger.info(f"Supplied market not (yet) available. Or wrong name. Use any of {marketlists.__all__}")

    # where possible call get_markets(extras=True) to limit the request number. #TODO implement in marketlists.py
    # if it doesn't make a difference request-wise than call get_market_extras later.

    # TODO make this use threads
    # The heart - the real requests.
    for market in checked_market_types:
      now = localtime()
      now_date = date(now.tm_year, now.tm_mon, now.tm_mday)
      market_type = getattr(marketlists, market).TYPE
      oldest_last_update = get_last_update(market_type, MARKET_DB_PATH, get_oldest=True)
      # Rewe runs in batches, can be stopped and later restarted at the same point.
      if market_type == "rewe" and os.path.exists(REWE_MARKETLIST_TEMP_ZIPCODE_RANGE):
        oldest_last_update = None
      # Penny markets have to be up to date, to get the correct sellingRegion for the week
      if market_type == "penny":
        week_start, week_end = set_week_start_and_end(now)
        if oldest_last_update < week_start:
          oldest_last_update = None
      # Do we need to create the markets (anew)? 
      if oldest_last_update == None or oldest_last_update + threshold_market_renewal <= now_date:
        # Delete market rows if supplied in force-list. UPDATE: too dangerous
        # if isinstance(force, list) and market_type in force:
        #   logger.info("Delete all markets for %s from markets-table.", market_type)
        #   delete_markets(MARKET_DB_PATH, market_type)
        # Some markets need extra handling.
        match market_type:
          # Rewe is special. Because of approx. 90000 requests, it is split into batches.
          case "rewe":
            status_code = 0
            while status_code == 0:
              zipcode_range, status_code = Markets._rewe_batches()
              logger.info("start rewe market query for zip range: %s", zipcode_range)
              markets = getattr(marketlists, market).get_markets(zipcode_range)
              store_markets(markets, MARKET_DB_PATH)
              logger.info("finished market query for zip range: %s", zipcode_range)
          case "penny" | "norma":
            markets, markets_extra, selling_regions_with_markets = getattr(marketlists, market).get_markets(extras=True)
            if markets != None and markets_extra != None and selling_regions_with_markets != None:
              store_markets(markets, MARKET_DB_PATH)
              store_market_extra_infos(markets_extra, MARKET_EXTRA_DB_PATH)
              store_selling_regions_with_markets(selling_regions_with_markets, SELLING_REGION_MARKETS_DB_PATH)
            else:
              logger.warning("%s failed", market_type)
              continue
          case _:
            markets = getattr(marketlists, market).get_markets()
            if markets != None:
              store_markets(markets, MARKET_DB_PATH)
            else:
              logger.warning("%s failed", market_type)
              continue
        print(market, "- all markets stored.")
      else:
        print(market, "is up to date. last_update:", oldest_last_update)

  @staticmethod
  @delay_range(120_000, 300_000) 
  def _rewe_batches(max_batch_size: int = 500) -> tuple[tuple[int, int], int]:
    """Don't overload the api, create batches with sleep in between.
    
    Returns the zip code range and a status code (0 = batches to do, 1 = last_batch)"""
    zip_range_start = 100
    zip_range_end = 99999
    zip_range_todo = (zip_range_start, zip_range_end)
    if os.path.exists(REWE_MARKETLIST_TEMP_ZIPCODE_RANGE):
      with open(REWE_MARKETLIST_TEMP_ZIPCODE_RANGE, "r") as file:
        zip_range_todo_loaded = json.load(file)
      # Check if file-value is valid.
      if len(zip_range_todo_loaded) == 2 \
          and isinstance(zip_range_todo_loaded[0], int) \
          and isinstance(zip_range_todo_loaded[1], int) \
          and zip_range_todo_loaded[0] <= zip_range_todo_loaded[1] \
          and zip_range_todo_loaded[0] >= zip_range_start \
          and zip_range_todo_loaded[1] <= zip_range_end:
        zip_range_todo = zip_range_todo_loaded
      else:
        logger.warning("Markets: Rewe temp file for market retrieval not valid, default to %s", zip_range_todo)
    
    batch_start, end = zip_range_todo
    batch_end = min(batch_start + max_batch_size - 1, end)
    batch =  (batch_start, batch_end)
    
    if batch_end == zip_range_end:
      os.remove(REWE_MARKETLIST_TEMP_ZIPCODE_RANGE)
      return batch, 1
    else:
      with open(REWE_MARKETLIST_TEMP_ZIPCODE_RANGE, "w") as file:
        json.dump((batch_end+1, end), file)
      return batch, 0



class Products:

  @staticmethod
  @delay_range(120_000, 300_000)
  def _batches(market_type: str, tmp_path: str, max_batch_size: int = 100) -> tuple[list[str], int]:
    """Don't overload the api, create batches with sleep in between.
    
    Returns a list of market ids and a status code (0 = batches to do, 1 = last_batch)"""
    if os.path.exists(tmp_path):
      with open(tmp_path, "r") as file:
        market_ids = json.load(file)
    else:
      market_ids = get_market_ids(market_type, MARKET_DB_PATH)
      if market_ids == None or len(market_ids) == 0:
        logger.warning(f"No {market_type} market-ids in market-table.")
        return None

    batch = sample(market_ids, k=min(max_batch_size, len(market_ids)))

    market_ids = [market_id for market_id in market_ids if market_id not in batch]  # maybe use a set

    if len(market_ids) == 0:
      os.remove(tmp_path)
      return batch, 1
    else:
      with open(tmp_path, "w") as file:
        json.dump(market_ids, file)
      return batch, 0
  
  @staticmethod
  def create_product_db(market_types: list[str] = discounts.__all__):

    # Was a market-list supplied?
    checked_market_types = list()
    for mt in market_types:
      if mt in discounts.__all__:
        checked_market_types.append(mt)
      else:
        logger.info(f"Supplied market not (yet) available. Or wrong name. Use any of {discounts.__all__}")
    
    week_start, _ = set_week_start_and_end(localtime())

    for market in checked_market_types:
      market_type = getattr(discounts, market).TYPE
      if products_present(market_type, week_start, PRODUCT_DB_PATH, light_check=True):
        update = False
      else:
        update = True
      if market_type == "rewe" and os.path.exists(REWE_DISCOUNTS_TEMP_MARKETS_TO_DO):
        update = True
      if market_type == "netto" and os.path.exists(NETTO_DISCOUNTS_TEMP_MARKETS_TO_DO):
        update = True
      if not update:
        continue
      logger.info("Starting discount retrieval for %s", market_type)
      match market_type:
        # access markets-table to get all rewe market ids; and use batches again.
        case "rewe" | "netto":
          if market_type == "rewe":
            temp_path = REWE_DISCOUNTS_TEMP_MARKETS_TO_DO
          elif market_type == "netto":
            temp_path = NETTO_DISCOUNTS_TEMP_MARKETS_TO_DO
          status_code = 0
          while status_code == 0:
            market_ids, status_code = Products._batches(market_type, temp_path)
            products, markets_with_product_ids = getattr(discounts, market).get_products(market_ids)
            if products == None or markets_with_product_ids == None:
              continue
            store_products(products, PRODUCT_DB_PATH)
            store_markets_with_product_ids(markets_with_product_ids, MARKET_PRODUCTS_DB_PATH)
        # access markets-table to get all hit market ids
        # TODO: integrate "hit" into "penny"/"norma"
        case "hit":
          hit_market_ids = get_market_ids(market_type, MARKET_DB_PATH)
          if hit_market_ids == None or len(hit_market_ids) == 0:
            logger.warning("No HIT market ids in markets-table.")
            continue
          products, markets_with_product_ids = getattr(discounts, market).get_products(hit_market_ids)
          if products == None or markets_with_product_ids == None:
            continue
          store_products(products, PRODUCT_DB_PATH)
          store_markets_with_product_ids(markets_with_product_ids, MARKET_PRODUCTS_DB_PATH)
        # access market_extra table to get selling regions
        case "penny" | "norma":
          now = localtime()
          week_start, _ = set_week_start_and_end(now)
          selling_regions = get_group_ids(market_type, week_start, SELLING_REGION_MARKETS_DB_PATH)
          if selling_regions == None or len(selling_regions) == 0:
            logger.warning("No selling regions for %s in market-extra-table.", market_type)
            continue
          products, selling_regions_with_product_ids = getattr(discounts, market).get_products(selling_regions)
          if products == None or selling_regions_with_product_ids == None:
            continue
          store_products(products, PRODUCT_DB_PATH)
          store_markets_with_product_ids(selling_regions_with_product_ids, MARKET_PRODUCTS_DB_PATH)
        case _:
          products = getattr(discounts, market).get_products()
          if products == None:
            continue
          store_products(products, PRODUCT_DB_PATH)
      logger.info("%s - product retrieval successful", market_type)
    

if __name__ == "__main__":
  Markets.create_market_db(["Norma"])
  # Products.create_product_db(["Netto"])