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
import time
import sqlite3
from pathlib import Path
import pprint

import httpx

from get_rewe_creds import get_creds
from product import Product

class Rewe:

  PRIVATE_KEY_FILENAME = "private.key"
  CERTIFICATE_FILENAME = "private.pem"
  SOURCE_PATH = Path(__file__).resolve().parent
  FULL_KEY_FILE_PATH = os.path.join(SOURCE_PATH, PRIVATE_KEY_FILENAME)
  FULL_CERT_FILE_PATH = os.path.join(SOURCE_PATH, CERTIFICATE_FILENAME)
  
  @staticmethod
  def load_rewe_market_ids() -> list[str]:
    # load from database
    con = sqlite3.connect("./rewe_markets.db")
    cur = con.cursor()
    cur.execute("select market_id from markets")
    market_ids = cur.fetchall()
    market_ids = [id[0] for id in market_ids]
    cur.close()
    con.close()
    return market_ids

  @staticmethod
  def get_products_with_id(market_id: str) -> list[Product]:
    """Get the products for one market id."""
    files = os.listdir(Rewe.SOURCE_PATH)
    if Rewe.PRIVATE_KEY_FILENAME not in files or Rewe.CERTIFICATE_FILENAME not in files:
        get_creds(source_path=Rewe.SOURCE_PATH, key_filename=Rewe.PRIVATE_KEY_FILENAME, cert_filename=Rewe.CERTIFICATE_FILENAME)
    
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
    res = httpx.Client(http2=True, cert=(client_cert, client_key), headers=header).get(url)
    if res.status_code != 200:
      print("problem with the api.")
      return None
    res = res.json()
    raw_products = res["data"]["offers"]
    all_offers = list()
    # valid_to
    valid_to = time.localtime(raw_products["untilDate"] / 1000)
    valid_to = time.strftime("%d.%m.%Y", valid_to)
    for category in raw_products["categories"]:
      # print(category["title"])
      if re.search(r".*PAYBACK.*", category["title"]):
        continue
      category_offers = category["offers"]
      for offer in category_offers:
        if offer["title"] == "":
          continue
        # pp = pprint.PrettyPrinter()
        # pp.pprint(offer)
        # break
        new_product = Product()
        new_product.name = offer["title"]
        new_product.valid_to = valid_to
        new_product.price = offer["priceData"]["price"]
        # detail.contents has a Art.-Nr. and can have a Hersteller and sometimes also a Herkunft
        for line in offer["detail"]["contents"]:
          if line["header"] == "Produktdetails":
            details = line["titles"]
        for line in details:
          if line.startswith("Art.-Nr."):
            pass
          if line.startswith("Hersteller"):
            new_product.producer = line.removeprefix("Hersteller:").strip()
          if line.startswith("Herkunft"):
            pass

        all_offers.append(new_product)

    return all_offers


  @staticmethod
  def get_products():
    """Get the products and store them."""
    pass


if __name__ == "__main__":
  # Rewe.load_rewe_market_ids()
  Rewe.get_products_with_id(market_id="840671")