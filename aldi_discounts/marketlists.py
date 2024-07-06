import os
import re
import uuid
import random
from pathlib import Path
from time import localtime

from httpx import Client
from bs4 import BeautifulSoup

from util import delay
from market import Market
from get_rewe_creds import get_rewe_creds

# TODO maybe add the opening hours
# and e.g. for aldi sued: "Parkplatz", "Meine Backwelt", "E-Tankstelle", "SelfCheckout"


class Aldi_sued:

  @staticmethod
  @delay
  def _get_market_details(market_link: str) -> Market:
    market = Market()
    res = Client(http2=True).get(market_link)
    parsed_link = BeautifulSoup(res.text, "html.parser")
    name = parsed_link.find(id="location-name").contents
    market.name = name[0]
    coords = parsed_link.find("span", class_="Address-coordinates")
    for coord in coords.children:
      if coord["itemprop"] == "latitude":
        market.latitude = coord["content"]
      elif coord["itemprop"] == "longitude":
        market.longitude = coord["content"]
    address = parsed_link.find("address", itemtype="http://schema.org/PostalAddress")
    postal_code = address.find("span", class_="Address-field Address-postalCode").contents
    market.postal_code = postal_code[0]
    city = address.find("span", class_="Address-field Address-city").contents
    market.city = city[0]
    address_line = address.find("span", class_="Address-field Address-line1").contents
    market.address = address_line[0]
    now = localtime()
    market.last_update = str(now.tm_year) + "-" + str(now.tm_yday)
    market.type = "aldi-sued"
    return market

  @staticmethod
  @delay
  def _unfold_multiple_markets(city_link_multiple_markets: str) -> list[str]:
    city_links = list()
    base_url = "https://filialen.aldi-sued.de/"
    res = Client(http2=True).get(city_link_multiple_markets)
    href_pattern = "(?<=../)" + city_link_multiple_markets.removeprefix(base_url) + "/.+?(?=\")"
    hrefs = re.findall(href_pattern, res.text)
    for inner_link in hrefs:
      city_link = base_url + inner_link
      city_links.append(city_link)
    return city_links
  
  @staticmethod
  @delay
  def _call_state_link(state_link: str) -> list[str]:
    city_links = list()
    city_links_multiple_markets = list()
    base_url = "https://filialen.aldi-sued.de/"
    res = Client(http2=True).get(state_link)
    href_pattern = state_link.removeprefix(base_url) + "/.+?(?=\")"
    data_count_pattern = state_link.removeprefix(base_url) + "/" + r".+?\bdata-count=\"\((?P<count>\d+)\)"
    hrefs = re.findall(href_pattern, res.text)
    href_data_counts = re.findall(data_count_pattern, res.text)
    # some market links do not refer to one market, but to a list of markets 
    # (usually when there are more than one market in a city)
    for index, inner_link in enumerate(hrefs):
      city_link = base_url + inner_link
      city_count = int(href_data_counts[index])
      if city_count == 1:
        city_links.append(city_link)
      elif city_count > 1:
        city_links_multiple_markets.append(city_link)
      else:
        print("Something is wrong with the pattern search: -> state links")
    for link in city_links_multiple_markets:
      links = Aldi_sued._unfold_multiple_markets_aldi_sued(link)
      city_links.extend(links)
    return city_links

  @staticmethod
  def create_marketlist() -> list[Market]:
    """Takes quiet a while (ca. 30 min) and uses a larger bit of memory.
    # TODO alternative: call store every hundret stores or so.
    """
    markets: list[Market] = list()
    url = "https://www.aldi-sued.de/de/filialen.html"
    res = Client(http2=True).get(url)
    pattern = r"https://filialen.aldi-sued.de/.+?(?=\?|/)"
    links = re.findall(pattern, res.text)
    state_links = list(set(links))
    city_links = list()

    # the markets are sorted under states (e.g. Hessen); loop over all states
    for link in state_links:
      links = Aldi_sued._call_state_link_aldi_sued(link)
      city_links.extend(links)

    for link in city_links:
      market = Aldi_sued._get_market_details_aldi_sued(link)
      markets.append(market)

    return markets


class Aldi_nord:

  @staticmethod
  def create_marketlist_aldi_nord() -> list[Market]:
    """API endpoint url can stop working. # TODO create a more robust version to get the api-url."""
    markets = list()
    url = "https://www.aldi-nord.de/filialen-und-oeffnungszeiten.html"
    res = Client(http2=True).get(url)
    parsed = BeautifulSoup(res.text, "html.parser")
    static_url_part1 = "https://locator.uberall.com/api/storefinders/"
    static_url_part3 = "/locations/all?"
    static_url_part5 = "&language=de&fieldMask=id&fieldMask=lat&fieldMask=lng&fieldMask=city&fieldMask=streetAndNumber&fieldMask=zip&fieldMask=addressExtra&fieldMask=name&"
    dynamic_url_part2 = parsed.find("div", id="store-finder-widget")["data-key"]
    url2 = "https://locator.uberall.com/locator-assets/storeFinderWidget-v2-withoutMap.js"
    res2 = Client(http2=True).get(url2)
    dynamic_url_part4 = re.findall(r"\?v=\d+", res2.text)[0]
    url3 = static_url_part1 + dynamic_url_part2 + static_url_part3 + dynamic_url_part4 + static_url_part5
    res3 = Client(http2=True).get(url3)
    if res3.status_code != 200:
      print("something went wrong with aldi nord: ", url3)
      return None
    res3 = res3.json()
    if res3["status"] != "SUCCESS":
      print("request of markets-json failed: ", res3["status"])
      return None
    for location in res3["response"]["locations"]:
      market = Market()
      market.type = "aldi-nord"
      market.postal_code = location["zip"]
      market.city = location["city"]
      if location["addressExtra"] != None:
        market.address = location["streetAndNumber"] + ", " + location["addressExtra"]
      else:
        market.address = location["streetAndNumber"]
      market.latitude = location["lat"]
      market.longitude = location["lng"]
      now = localtime()
      market.last_update = str(now.tm_year) + "-" + str(now.tm_yday)
      market.name = location["name"]
      market.id = location["id"]
      markets.append(market)
    return markets


class Penny:
  
  @staticmethod
  def create_marketlist_penny() -> list[Market]:
    url = "https://www.penny.de/.rest/market"
    res = Client(http2=True).get(url)
    if res.status_code != 200:
      print("something wrong with penny market list request")
      return None
    all_markets: list[Market] = list()
    markets = res.json()
    for m in markets:
      market = Market()
      market.id = m["sellingRegion"]
      market.type = "penny"
      market.name = m["marketName"]
      market.address = m["streetWithHouseNumber"]
      market.city = m["city"]
      market.postal_code = m["zipCode"]
      market.latitude = m["latitude"]
      market.longitude = m["longitude"]
      now = localtime()
      market.last_update = str(now.tm_year) + "-" + str(now.tm_yday)
      all_markets.append(market)

    return all_markets


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
  pass