import requests
import re
import time
import random
import sqlite3

from bs4 import BeautifulSoup

# TODO maybe add the opening hours
# and e.g. for aldi sued: "Parkplatz", "Meine Backwelt", "E-Tankstelle", "SelfCheckout"

COLUMNS = ["market_type", "postal_code", "city", "address", "latitude", "longitude", "last_update"]

def store_markets(markets: list[dict]):
  if len(markets) > 0:
    con = sqlite3.connect("./markets.db")
    cursor = con.cursor()
    market_tuples = list()
    for market in markets:
      market_tuple = (market["market_type"], market["postal_code"], market["city"], market["address"], market["latitude"], market["longitude"], market["last_update"])
      market_tuples.append(market_tuple)
    cursor.execute(f"CREATE TABLE if not exists markets({','.join(COLUMNS)})")
    cursor.executemany(f"INSERT INTO markets VALUES({','.join(['?']*len(COLUMNS))})", market_tuples)
    con.commit()
    cursor.close()
    con.close()

def create_marketlist_aldi_sued():
  """Takes quiet a while (ca. 30 min) and uses a larger bit of memory.
  
  # TODO alternative: call store every hundret stores or so.
  """
  markets = list()
  url = "https://www.aldi-sued.de/de/filialen.html"
  res = requests.get(url)
  pattern = r"https://filialen.aldi-sued.de/.+?(?=\?|/)"
  links = re.findall(pattern, res.text)
  state_links = list(set(links))
  base_link = "https://filialen.aldi-sued.de/"
  city_links = list()
  city_links_multiple_markets = list()
  for link in state_links:
    print(link)
    res = requests.get(link)
    href_pattern = link.removeprefix(base_link) + "/.+?(?=\")"
    data_count_pattern = link.removeprefix(base_link) + "/" + r".+?\bdata-count=\"\((?P<count>\d+)\)"
    hrefs = re.findall(href_pattern, res.text)
    href_data_counts = re.findall(data_count_pattern, res.text)
    for index, inner_link in enumerate(hrefs):
      city_link = base_link + inner_link
      city_count = int(href_data_counts[index])
      if city_count == 1:
        city_links.append(city_link)
      elif city_count > 1:
        city_links_multiple_markets.append(city_link)
      else:
        print("Something is wrong with the pattern search: -> state links")

  for link in city_links_multiple_markets:
    res = requests.get(link)
    href_pattern = "(?<=../)" + link.removeprefix(base_link) + "/.+?(?=\")"
    hrefs = re.findall(href_pattern, res.text)
    for inner_link in hrefs:
      city_link = base_link + inner_link
      city_links.append(city_link)

  for link in city_links:
    sleeptime = random.randint(0, 10)
    time.sleep(sleeptime / 10)
    market = dict()
    res = requests.get(link)
    parsed_link = BeautifulSoup(res.text, "html.parser")
    coords = parsed_link.find("span", class_="Address-coordinates")
    for coord in coords.children:
      market[coord["itemprop"]] = coord["content"]
    address = parsed_link.find("address", itemtype="http://schema.org/PostalAddress")
    postal_code = address.find("span", class_="Address-field Address-postalCode").contents
    market["postal_code"] = postal_code[0]
    city = address.find("span", class_="Address-field Address-city").contents
    market["city"] = city[0]
    address_line = address.find("span", class_="Address-field Address-line1").contents
    market["address"] = address_line[0]
    now = time.localtime()
    market["last_update"] = str(now.tm_year) + "-" + str(now.tm_yday)
    market["market_type"] = "aldi-sued"
    markets.append(market)
    print(market)
  store_markets(markets)


def create_marketlist_aldi_nord():
  """API endpoint url can stop working. # TODO create a more robust version to get the api-url."""
  markets = list()
  url = "https://www.aldi-nord.de/filialen-und-oeffnungszeiten.html"
  res = requests.get(url)
  parsed = BeautifulSoup(res.text, "html.parser")
  static_url_part1 = "https://locator.uberall.com/api/storefinders/"
  static_url_part3 = "/locations/all?"
  static_url_part5 = "&language=de&fieldMask=id&fieldMask=lat&fieldMask=lng&fieldMask=city&fieldMask=streetAndNumber&fieldMask=zip&fieldMask=addressExtra&"
  dynamic_url_part2 = parsed.find("div", id="store-finder-widget")["data-key"]
  url2 = "https://locator.uberall.com/locator-assets/storeFinderWidget-v2-withoutMap.js"
  res2 = requests.get(url2)
  dynamic_url_part4 = re.findall(r"\?v=\d+", res2.text)[0]
  url3 = static_url_part1 + dynamic_url_part2 + static_url_part3 + dynamic_url_part4 + static_url_part5
  res3 = requests.get(url3)
  if res3.status_code != 200:
    print("something went wrong with aldi nord: ", url3)
    return "error"
  res3 = res3.json()
  if res3["status"] != "SUCCESS":
    print("request of markets-json failed: ", res3["status"])
    return "error"
  for location in res3["response"]["locations"]:
    market = dict()
    market["market_type"] = "aldi-nord"
    market["postal_code"] = location["zip"]
    market["city"] = location["city"]
    if location["addressExtra"] != None:
      market["address"] = location["streetAndNumber"] + ", " + location["addressExtra"]
    else:
      market["address"] = location["streetAndNumber"]
    market["latitude"] = location["lat"]
    market["longitude"] = location["lng"]
    now = time.localtime()
    market["last_update"] = str(now.tm_year) + "-" + str(now.tm_yday)
    markets.append(market)
  store_markets(markets)


if __name__ == "__main__":
  pass