import os
import uuid
import random
from pathlib import Path
from time import localtime

from httpx import Client

from market import Market
from util import delay
from get_rewe_creds import get_rewe_creds


class Rewe:

  PRIVATE_KEY_FILENAME = "private.key"
  CERTIFICATE_FILENAME = "private.pem"
  SOURCE_PATH = Path(__file__).resolve().parent
  FULL_KEY_FILE_PATH = os.path.join(SOURCE_PATH, PRIVATE_KEY_FILENAME)
  FULL_CERT_FILE_PATH = os.path.join(SOURCE_PATH, CERTIFICATE_FILENAME)

  @staticmethod
  @delay
  def _get_market_details(market: Market) -> Market:
    """add opening hours"""

    return market

  @staticmethod
  @delay
  def _get_markets(zip_code: str) -> list[Market]:
    files = os.listdir(Rewe.SOURCE_PATH)
    if Rewe.PRIVATE_KEY_FILENAME not in files or Rewe.CERTIFICATE_FILENAME not in files:
        get_rewe_creds(source_path=Rewe.SOURCE_PATH, key_filename=Rewe.PRIVATE_KEY_FILENAME, cert_filename=Rewe.CERTIFICATE_FILENAME)
    
    client_cert = Rewe.FULL_CERT_FILE_PATH
    client_key = Rewe.FULL_KEY_FILE_PATH
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
    res = Client(http2=True, cert=(client_cert, client_key), headers=header).get(url)
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
      market = Market()
      market.id = m["id"]
      market.type = "rewe"
      market.name = m["name"]
      market.postal_code = m["rawValues"]["postalCode"]
      market.city = m["rawValues"]["city"]
      market.address = m["addressLine1"]
      market.latitude = m["location"]["latitude"]
      market.longitude = m["location"]["longitude"]
      now = localtime()
      market.last_update = str(now.tm_year) + "-" + str(now.tm_yday)
      all_markets.append(market)
    return all_markets

  @staticmethod
  def create_marketlist_rewe() -> list[Market]:
    all_markets: list[Market] = list()
    all_zipcodes = [i for i in range(10000,99999)]
    while len(all_zipcodes) != 0:
      zipcode = random.choice(all_zipcodes)
      markets = Rewe._get_markets(str(zipcode))
      if markets != None:
        for market in markets:
          if market.id not in [m.id for m in all_markets]:
            all_markets.append(market)
        break  # TODO for test, remove later
      all_zipcodes.remove(zipcode)
    return all_markets

if __name__ == "__main__":
  ms = Rewe.create_marketlist_rewe()
  for i in ms:
    print(i)