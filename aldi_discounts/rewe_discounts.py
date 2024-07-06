"""
Since every rewe market can have different discounts we need a way to cut storage
a bit. Because with approx. 3700 markets in Germany and approx. 300 discounts
per market we have 1.110.000 discounts to store per week. 
Use the unique id of every product: 
 in one table we have the products
 and in another we have the markets and the product-ids.
 -> maybe we find a way to only call certain markets (figure out the markets, 
 that always have the same discounts) 
"""

import os
import re
import uuid
from time import localtime, strftime
import sqlite3
from pathlib import Path

from httpx import Client

from util import delay
from product import Product
from get_rewe_creds import get_rewe_creds

class Rewe:

  COLUMNS = ["market_id", "offer_ids", "last_update"]

  PRIVATE_KEY_FILENAME = "private.key"
  CERTIFICATE_FILENAME = "private.pem"
  SOURCE_PATH = Path(__file__).resolve().parent
  FULL_KEY_FILE_PATH = os.path.join(SOURCE_PATH, PRIVATE_KEY_FILENAME)
  FULL_CERT_FILE_PATH = os.path.join(SOURCE_PATH, CERTIFICATE_FILENAME)

  @staticmethod
  def store_market_with_offer_ids(market_id: str, offer_ids: list[str], last_update: str):
    offer_ids.sort(key=lambda id: int(id))
    con = sqlite3.connect("./rewe_market_with_offer_ids.db")
    cur = con.cursor()
    cur.execute(f"CREATE TABLE if not exists market_with_offer_ids({','.join(Rewe.COLUMNS)})")
    cur.execute(f"INSERT INTO market_with_offer_ids({','.join(Rewe.COLUMNS)}) VALUES('{market_id}', '{','.join(offer_ids)}', '{last_update}')")
    con.commit()
    cur.close()
    con.close()

  @staticmethod
  def load_rewe_market_ids(market_db_path: str) -> list[str]:
    # load from database
    con = sqlite3.connect(market_db_path)
    cur = con.cursor()
    cur.execute("SELECT market_id FROM markets")
    market_ids = cur.fetchall()
    market_ids = [id[0] for id in market_ids]
    cur.close()
    con.close()
    return market_ids

  @staticmethod
  @delay
  def get_products_with_market_id(market_id: str) -> tuple[list[Product], list[str]]:
    """Get the products for one market id."""
    files = os.listdir(Rewe.SOURCE_PATH)
    if Rewe.PRIVATE_KEY_FILENAME not in files or Rewe.CERTIFICATE_FILENAME not in files:
        get_rewe_creds(source_path=Rewe.SOURCE_PATH, key_filename=Rewe.PRIVATE_KEY_FILENAME, cert_filename=Rewe.CERTIFICATE_FILENAME)
    
    client_cert = Rewe.FULL_CERT_FILE_PATH
    client_key = Rewe.FULL_KEY_FILE_PATH
    hostname = "mobile-clients-api.rewe.de"
    url = "https://" + hostname + "/api/stationary-app-offers/" + str(market_id)
    rdfa_uuid = str(uuid.uuid4())
    correlation_id_uuid = str(uuid.uuid4())
    header = {
      # "ruleVersion": "2",
      "user-agent": "REWE-Mobile-Client/3.17.1.32270 Android/11 Phone/Google_sdk_gphone_x86_64",
      "rdfa": rdfa_uuid,  #"d53d57e6-1f5a-4112-94aa-d900c1dc1556",
      "Correlation-Id": correlation_id_uuid,  #"c0147af1-8f04-49e8-b573-425c33b963b1",
      "rd-service-types": "UNKNOWN",
      "x-rd-service-types": "UNKNOWN",
      "rd-is-lsfk": "false",
      "rd-customer-zip": "",
      "rd-postcode": "",
      "x-rd-customer-zip": "",
      "rd-market-id": "",
      "x-rd-market-id": "",
      "a-b-test-groups": "productlist-citrusad",
      "Host": hostname,
      "Connection": "Keep-Alive",
      "Accept-Encoding": "gzip",
      }
    res = Client(http2=True, cert=(client_cert, client_key), headers=header).get(url)
    if res.status_code != 200:
      print("problem with the api.")
      return None
    res = res.json()
    raw_products = res["data"]["offers"]
    all_offers = list()
    all_offer_ids = list()
    # valid_to
    valid_to = localtime(raw_products["untilDate"] / 1000)
    valid_to = strftime("%d.%m.%Y", valid_to)
    for category in raw_products["categories"]:
      if re.search(r".*PAYBACK.*", category["title"]):
        continue
      category_offers = category["offers"]
      for offer in category_offers:
        if offer["title"] == "":
          continue
        new_product = Product()
        # name
        new_product.name = offer["title"]
        # valid_to
        new_product.valid_to = valid_to
        # price
        new_product.price = offer["priceData"]["price"]
        # description
        new_product.description = offer["subtitle"]
        # detail.contents has a Art.-Nr. and can have a Hersteller and sometimes also a Herkunft
        for line in offer["detail"]["contents"]:
          if line["header"] == "Produktdetails":
            details = line["titles"]
        for line in details:
          if line.startswith("Art.-Nr."):
            # unique_id
            new_product.unique_id = line.removeprefix("Art.-Nr.:").strip()
          if line.startswith("Hersteller"):
            # producer
            new_product.producer = line.removeprefix("Hersteller:").strip()
          if line.startswith("Herkunft"):
            # origin
            new_product.origin = line.removeprefix("Herkunft:").strip()
        # offer["detail"]["nutriScore"]
        # offer["detail"]["pitchIn"] <- 'Nur in der Bedienungstheke', 'Mit App günstiger'

        # valid_to, price_before, link <- missing
        all_offers.append(new_product)
        all_offer_ids.append(new_product.unique_id)

    return all_offers, all_offer_ids


  @staticmethod
  @delay
  def get_product_details(product: Product, product_id: str = None) -> Product:
    if product_id == None:
      product_id = product.unique_id
    # TODO: call different rewe api
    return product
    

  @staticmethod
  def get_products(market_db_path: str) -> list[Product]:
    """Get the products from every market and stores the article ids together with the market id.
    Returns all unique offers.
    
    all_offers contains the parsed offers 
    -> TODO: for every product/offer we need to call a different api-endpoint
    """
    now = localtime()
    last_update = str(now.tm_year) + "-" + str(now.tm_yday)
    market_ids = Rewe.load_rewe_market_ids(market_db_path)
    all_offers = set()
    all_offer_ids = set()
    for market_id in market_ids:
      market_offers, market_offer_ids = Rewe.get_products_with_market_id(market_id)
      all_offers.update(set(market_offers))
      all_offer_ids.update(set(market_offer_ids))
      # write the offer ids together with the market id in a table
      Rewe.store_market_with_offer_ids(market_id, list(set(market_offer_ids)), last_update)
    #all_offers = map(Rewe.get_product_details, all_offers)
    all_offers = list(all_offers)
    return all_offers



if __name__ == "__main__":
  # Rewe.load_rewe_market_ids()
  # Rewe.get_products_with_id(market_id="840671")
  Rewe.get_products()
  