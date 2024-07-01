import os
import time
import uuid
import random
import sqlite3
from pathlib import Path

import requests
import httpx

from get_rewe_creds import get_creds

COLUMNS = ["market_id", "market_name", "postal_code", "city", "address", "latitude", "longitude", "last_update"]

PRIVATE_KEY_FILENAME = "private.key"
CERTIFICATE_FILENAME = "private.pem"
SOURCE_PATH = Path(__file__).resolve().parent
FULL_KEY_FILE_PATH = os.path.join(SOURCE_PATH, PRIVATE_KEY_FILENAME)
FULL_CERT_FILE_PATH = os.path.join(SOURCE_PATH, CERTIFICATE_FILENAME)

def get_markets(zip_code: str) -> list[dict]:
  files = os.listdir(SOURCE_PATH)
  if PRIVATE_KEY_FILENAME not in files or CERTIFICATE_FILENAME not in files:
      get_creds(source_path=SOURCE_PATH, key_filename=PRIVATE_KEY_FILENAME, cert_filename=CERTIFICATE_FILENAME)
  
  client_cert = FULL_CERT_FILE_PATH
  client_key = FULL_KEY_FILE_PATH
  hostname = "mobile-api.rewe.de"
  url = "https://" + hostname + "/api/v3/market/search?search=" + str(zip_code)
  rdfa_uuid = str(uuid.uuid4())
  correlation_id_uuid = str(uuid.uuid4())
  header = {
    "ruleVersion": "2",
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
    "Accept-Encoding": "gzip"
    }
  all_markets = list()
  res = httpx.Client(http2=True, cert=(client_cert, client_key), headers=header).get(url)
  if res.status_code != 200:
    print("rewe api call failed")
    return None
  res = res.json()
  markets = res["markets"]
  # extract wwident, name, postal_code, city, address, latitude, longitude 
  # add last_update (year-dayoftheyear)
  if len(markets) == 0:
    # print("no markets found for: ", zip_code)
    return None
  for m in markets:
    market = dict()
    market["market_id"] = m["id"]
    market["market_name"] = m["name"]
    market["postal_code"] = m["rawValues"]["postalCode"]
    market["city"] = m["rawValues"]["city"]
    market["address"] = m["addressLine1"]
    market["latitude"] = m["location"]["latitude"]
    market["longitude"] = m["location"]["longitude"]
    now = time.localtime()
    market["last_update"] = str(now.tm_year) + "-" + str(now.tm_yday)
    all_markets.append(market)
  return all_markets

def store_markets(markets: list[dict]):
  if len(markets) > 0:
    con = sqlite3.connect("./rewe_markets.db")
    # TODO: clean markets (remove already gotten)
    cursor = con.cursor()
    market_tuples = list()
    for market in markets:
      market_tuple = (market["market_id"], market["market_name"], market["postal_code"], market["city"], market["address"], market["latitude"], market["longitude"], market["last_update"])
      market_tuples.append(market_tuple)
    cursor.execute(f"CREATE TABLE if not exists markets({','.join(COLUMNS)})")
    cursor.executemany(f"INSERT INTO markets VALUES({','.join(['?']*len(COLUMNS))})", market_tuples)
    con.commit()
    cursor.close()
    con.close()

def create_marketlist_rewe():
  all_zipcodes = [i for i in range(10000,99999)]
  while len(all_zipcodes) != 0:
    zipcode = random.choice(all_zipcodes)
    markets = get_markets(str(zipcode))
    if markets != None:
      store_markets(markets)
      break
    all_zipcodes.remove(zipcode)

if __name__ == "__main__":
  create_marketlist_rewe()