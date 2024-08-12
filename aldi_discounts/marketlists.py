import os
import re
import uuid
import random
import logging
from pathlib import Path
from datetime import date
from time import localtime

from httpx import Client
from bs4 import BeautifulSoup

from market import Market
from market_extra import Market_Extra
from util import delay_range, get_rewe_creds, setup_logger

__all__ = ["Aldi_sued", "Aldi_nord", "Hit", "Penny", "Rewe"]

# TODO add extra markets infos
# and e.g. for aldi sued: "Parkplatz", "Meine Backwelt", "E-Tankstelle", "SelfCheckout", "Coffee To Go"

# Create logger for marketlists
LOG_PATH_FOLDERNAME = "log"
DATA_PATH_FOLDERNAME = "data"
SOURCE_PATH = Path(__file__).resolve().parent
if not (os.path.exists(LOG_PATH := os.path.join(SOURCE_PATH, LOG_PATH_FOLDERNAME))):
  os.mkdir(LOG_PATH)
if not (os.path.exists(DATA_PATH := os.path.join(SOURCE_PATH, DATA_PATH_FOLDERNAME))):
  os.mkdir(DATA_PATH)

logger = logging.getLogger(__name__)
log_handler = logging.FileHandler(os.path.join(LOG_PATH, f"{__name__}.log"), mode="w", encoding="utf-8")
log_formatter = logging.Formatter("%(name)s (%(levelname)s) - %(asctime)s - %(message)s")
log_handler.setFormatter(log_formatter)
logger.addHandler(log_handler)


class Market_Base:
  TYPE = ""
  LOGGER = None
  
  @staticmethod
  def get_markets_extra() -> list[Market_Extra]:
    pass

  @staticmethod
  def get_markets(*, loglevel: str = "DEBUG", one_logger_file: bool = True, logger_file_mode: str = "w") -> list[Market]:
    Market_Base.LOGGER = setup_logger(SOURCE_PATH, __name__, Market_Base.TYPE, loglevel, one_logger_file, logger_file_mode)
    pass


class Aldi_sued:

  TYPE = "aldi-sued"
  LOGGER = None

  @staticmethod
  @delay_range()
  def _get_market_details(market_link: str) -> Market:
    market = Market()
    try:
      res = Client(http2=True).get(market_link)
    except:
      Aldi_sued.LOGGER.warning("Problem with request to: %s", market_link)
      return None
    if res.status_code != 200:
      Aldi_sued.LOGGER.warning("Unexpected status code %s from request to %s", res.status_code, market_link)
      return None
    parsed = BeautifulSoup(res.text, "html.parser")
    name = coordinates = latitude = longitude = address = postal_code = city = street_and_number = id = None
    if not ((name := parsed.find(id="location-name")) and (len(name.contents) != 0) and (name := name.contents[0]) and
            (coordinates := parsed.find("span", class_="Address-coordinates")) and 
            (latitude := coordinates.find(lambda tag: tag.has_attr("itemprop") and tag.has_attr("content") and tag.get("itemprop") == "latitude")) and (latitude := latitude.get("content")) and
            (longitude := coordinates.find(lambda tag: tag.has_attr("itemprop") and tag.has_attr("content") and tag.get("itemprop") == "longitude")) and (longitude := longitude.get("content")) and
            (address := parsed.find("address", itemtype="http://schema.org/PostalAddress")) and
            (postal_code := address.find("span", class_="Address-field Address-postalCode")) and (len(postal_code.contents) != 0) and (postal_code := postal_code.contents[0]) and
            (city := address.find("span", class_="Address-field Address-city")) and (len(city.contents) != 0) and (city := city.contents[0]) and
            (street_and_number := address.find("span", class_="Address-field Address-line1")) and (len(street_and_number.contents) != 0) and (street_and_number := street_and_number.contents[0]) and
            (id := parsed.find(lambda tag: tag.get("id") == "main" and tag.get("itemid"))) and (id := id.get("itemid")) and (len(id.split("#")) == 2) and (id := id.split("#")[-1])):
      Aldi_sued.LOGGER.warning("Problem with parsing the market: \
                     \nname=%s, coordinates=%s, latitude=%s, longitude=%s, address=%s, postal_code=%s, city=%s, street_and_number=%s, id=%s",
                     name, coordinates, latitude, longitude, address, postal_code, city, street_and_number, id)
      return None
    
    market.name = name
    market.latitude = latitude
    market.longitude = longitude
    market.postal_code = postal_code
    market.city = city
    market.address = street_and_number
    market.id = id
    now = localtime()
    market.last_update = date(now.tm_year, now.tm_mon, now.tm_mday)
    market.type = Aldi_sued.TYPE
    return market    

  @staticmethod
  @delay_range()
  def _unfold_multiple_markets(multiple_markets_link: str) -> list[str]:
    Aldi_sued.LOGGER.info("Unfolding city... %s", multiple_markets_link)
    market_links = list()
    base_url = "https://filialen.aldi-sued.de/"
    try:
      res = Client(http2=True).get(multiple_markets_link)
    except:
      Aldi_sued.LOGGER.warning("Problem with request to: %s", multiple_markets_link)
      return None
    if res.status_code != 200:
      Aldi_sued.LOGGER.warning("Unexpected status code %s from request to %s", res.status_code, multiple_markets_link)
      return None
    href_pattern = "(?<=../)" + multiple_markets_link.removeprefix(base_url) + "/.+?(?=\")"
    hrefs = re.findall(href_pattern, res.text)
    if len(hrefs) == 0:
      Aldi_sued.LOGGER.warning("No links found in %s", multiple_markets_link)
      return None
    for inner_link in hrefs:
      city_link = base_url + inner_link
      market_links.append(city_link)
    Aldi_sued.LOGGER.info("Unfolded!")
    return market_links
  
  @staticmethod
  @delay_range()
  def _call_state_link(state_link: str) -> list[str]:
    """Get a link for every market."""
    Aldi_sued.LOGGER.info("Processing state... %s", state_link)
    market_links = list()
    multiple_markets_links = list()
    base_url = "https://filialen.aldi-sued.de/"
    try:
      res = Client(http2=True).get(state_link)
    except:
      Aldi_sued.LOGGER.warning("Problem with request to: %s", state_link)
      return None
    if res.status_code != 200:
      Aldi_sued.LOGGER.warning("Unexpected status code %s from request to %s", res.status_code, state_link)
      return None
    href_pattern = state_link.removeprefix(base_url) + "/.+?(?=\")"
    data_count_pattern = state_link.removeprefix(base_url) + "/" + r".+?\bdata-count=\"\((?P<count>\d+)\)"
    hrefs = re.findall(href_pattern, res.text)
    if len(hrefs) == 0:
      Aldi_sued.LOGGER.warning("No markets found for url: %s", state_link)
      return None
    href_data_counts = re.findall(data_count_pattern, res.text)
    if len(href_data_counts) == 0:
      Aldi_sued.LOGGER.warning("Count for markets not found for url: %s", state_link)
      return None
    elif len(href_data_counts) != len(hrefs):
      Aldi_sued.LOGGER.warning("Number of markets found and market-count do not match, url: %s", state_link)
      return None
    # some market links do not refer to one market, but to a list of markets 
    # (usually when there are more than one market in a city)
    for index, partial_market_link in enumerate(hrefs):
      market_link = base_url + partial_market_link
      try:
        number_of_markets_in_city = int(href_data_counts[index])
      except:
        Aldi_sued.LOGGER.warning("Problem with int-casting the count of market %s", market_link)
        return None
      if number_of_markets_in_city == 1:
        market_links.append(market_link)
      elif number_of_markets_in_city > 1:
        multiple_markets_links.append(market_link)
      else:
        Aldi_sued.LOGGER.warning("Count of market %s must be 1 or higher", market_link)
        return None
    for link in multiple_markets_links:
      links = Aldi_sued._unfold_multiple_markets(link)
      if links != None:
        market_links.extend(links)
    Aldi_sued.LOGGER.info("Processed!")
    return market_links

  @staticmethod
  def get_markets(*, loglevel: str = "DEBUG", one_logger_file: bool = True, logger_file_mode: str = "w") -> list[Market]:
    """Takes quiet a while (ca. 30 min) and uses a larger bit of memory."""
    Aldi_sued.LOGGER = setup_logger(SOURCE_PATH, __name__, Aldi_sued.TYPE, loglevel, one_logger_file, logger_file_mode)

    markets: list[Market] = list()
    url = "https://www.aldi-sued.de/de/filialen.html"
    try:
      res = Client(http2=True).get(url)
    except:
      Aldi_sued.LOGGER.warning("Problem with request to: %s", url)
      return None
    if res.status_code != 200:
      Aldi_sued.LOGGER.warning("Unexpected status code %s from request to %s", res.status_code, url)
      return None
    pattern = r"https://filialen.aldi-sued.de/.+?(?=\?|/)"
    links = re.findall(pattern, res.text)
    if len(links) == 0:
      Aldi_sued.LOGGER.warning("No state links found: %s - Nothing to do..", url)
      return None
    state_links = list(set(links))
    city_links = list()

    # the markets are sorted under states (e.g. Hessen); loop over all states
    for link in state_links:
      links = Aldi_sued._call_state_link(link)
      if links != None:
        city_links.extend(links)

    Aldi_sued.LOGGER.info("Starting market requests...")
    for link in city_links:
      market = Aldi_sued._get_market_details(link)
      if market != None:
        markets.append(market)
    Aldi_sued.LOGGER.info("Done!")

    return markets


class Aldi_nord:
  """Three request."""

  TYPE = "aldi-nord"
  LOGGER = None

  @staticmethod
  def get_markets(*, loglevel: str = "DEBUG", one_logger_file: bool = True, logger_file_mode: str = "w") -> list[Market]:
    """API endpoint url can stop working. # TODO create a more robust version to get the api-url."""
    Aldi_nord.LOGGER = setup_logger(SOURCE_PATH, __name__, Aldi_nord.TYPE, loglevel, one_logger_file, logger_file_mode)

    markets = list()
    url = "https://www.aldi-nord.de/filialen-und-oeffnungszeiten.html"
    try:
      res = Client(http2=True).get(url)
    except:
      Aldi_nord.LOGGER.warning("Problem with request to: %s", url)
    if res.status_code != 200:
      Aldi_nord.LOGGER.warning("Unexpected status code %s from request to %s", res.status_code, url)
      return None
    parsed = BeautifulSoup(res.text, "html.parser")
    static_url_part1 = "https://locator.uberall.com/api/storefinders/"
    static_url_part3 = "/locations/all?"
    static_url_part5 = "&language=de&fieldMask=id&fieldMask=lat&fieldMask=lng&fieldMask=city&fieldMask=streetAndNumber&fieldMask=zip&fieldMask=addressExtra&fieldMask=name&"
    dynamic_url_part2 = parsed.find("div", id="store-finder-widget")["data-key"]
    if dynamic_url_part2 == None:
      Aldi_nord.LOGGER.warning("Couldn't find part of the url. Problem with: %s", url)
      return None
    dynamic_url_part2 = str(dynamic_url_part2)
    url2 = "https://locator.uberall.com/locator-assets/storeFinderWidget-v2-withoutMap.js"
    try:
      res2 = Client(http2=True).get(url2)
    except:
      Aldi_nord.LOGGER.warning("Problem with request to: %s", url2)
      return None
    if res2.status_code != 200:
      Aldi_nord.LOGGER.warning("Unexpected status code %s from request to %s", res2.status_code, url2)
      return None
    dynamic_url_part4 = re.findall(r"\?v=\d+", res2.text)
    if len(dynamic_url_part4) == 0:
      Aldi_nord.LOGGER.warning("Couldn't find part of the url. Problem with: %s", url2)
      return None
    dynamic_url_part4 = dynamic_url_part4[0]
    url3 = static_url_part1 + dynamic_url_part2 + static_url_part3 + dynamic_url_part4 + static_url_part5
    try:
      res3 = Client(http2=True).get(url3)
    except:
      Aldi_nord.LOGGER.warning("Problem with request to: %s", url3)
      return None
    if res3.status_code != 200:
      Aldi_nord.LOGGER.warning("Unexpected status code %s from request to %s", res3.status_code, url3)
      return None
    try:
      res3 = res3.json()
    except:
      Aldi_nord.LOGGER.warning("Couldn't parse %s to a json", url3)
      return None
    if res3["status"] != "SUCCESS":
      Aldi_nord.LOGGER.warning("Request to %s failed with code %s", url3, res3["status"])
      return None
    if (res3_response := res3.get("response")) == None:
      Aldi_nord.LOGGER.warning("Property 'response' not found in json from %s", url3)
      return None
    if (res3_locations := res3_response.get("locations")) == None:
      Aldi_nord.warning("Property 'locations' not found in json from %s", url3)
      return None
    for location in res3_locations:
      zip_code = city = street_and_number = latitude = longitude = name = id = None
      if not ((zip_code := location.get("zip")) and 
              (city := location.get("city")) and 
              (street_and_number := location.get("streetAndNumber")) and
              (latitude := location.get("lat")) and
              (longitude := location.get("lng")) and
              (name := location.get("name")) and
              (id := location.get("id"))):
        Aldi_nord.warning("Problem with parsing the market: \
                       \nzip=%s, city=%s, streetAndNumber=%s, latitude=%s, longitude=%s, name=%s, id=%s \
                       \noriginal json: %s", 
                       zip_code, city, street_and_number, latitude, longitude, name, id, location)
        return None
      market = Market()
      market.type = Aldi_nord.TYPE
      market.postal_code = location["zip"]
      market.city = location["city"]
      if location.get("addressExtra") != None:
        market.address = location["streetAndNumber"] + ", " + location["addressExtra"]
      else:
        market.address = location["streetAndNumber"]
      market.latitude = location["lat"]
      market.longitude = location["lng"]
      now = localtime()
      market.last_update = date(now.tm_year, now.tm_mon, now.tm_mday)
      market.name = location["name"]
      market.id = location["id"]
      markets.append(market)
    return markets


# run every time before running discounts.Penny
class Penny:
  """One request."""
  
  TYPE = "penny"
  LOGGER = None

  @staticmethod
  def get_raw():
    url = "https://www.penny.de/.rest/market"
    try:
      res = Client(http1=True, timeout=30.0).get(url)
    except:
      Penny.LOGGER.warning("Problem with request to: %s", url)
      return None
    if res.status_code != 200:
      Penny.LOGGER.warning("Unexpected status code %s from request to %s", res.status_code, url)
      return None
    try:
      markets_raw = res.json()
    except:
      Penny.LOGGER.warning("Couldn't parse %s to a json", url)
      return None
    return markets_raw

  @staticmethod
  def get_markets_extra(raw_data = None) -> list[Market_Extra]:
    if raw_data == None:
      raw_data = Penny.get_raw()
      if raw_data == None:
        return None
    markets_extra: list[Market_Extra] = list()
    for market_extra_raw in raw_data:
      id = selling_region = None
      if not ((id := market_extra_raw.get("wwIdent")) and (selling_region := market_extra_raw.get("sellingRegion"))):
        Penny.LOGGER.warning("Penny: Problem with getting id=%s and selling_region=%s in market extra.",
                       id, selling_region)
        return None
      market_extra = Market_Extra()
      market_extra.type = Penny.TYPE
      market_extra.id = market_extra_raw["wwIdent"]
      market_extra.group_id = market_extra_raw["sellingRegion"]
      # TODO opening times etc.
      now = localtime()
      market_extra.last_update = date(now.tm_year, now.tm_mon, now.tm_mday)
      markets_extra.append(market_extra)
    
    return markets_extra

  @staticmethod
  def get_markets(extras: bool = True, *, loglevel: str = "DEBUG", one_logger_file: bool = True, logger_file_mode: str = "w") -> list[Market]:
    Penny.LOGGER = setup_logger(SOURCE_PATH, __name__, Penny.TYPE, loglevel, one_logger_file, logger_file_mode)

    markets_raw = Penny.get_raw()
    if markets_raw == None:
      return None
    markets: list[Market] = list()
    for market_raw in markets_raw:
      id = market_name = street_with_house_number = city = zip_code = latitude = longitude = None
      if not ((id := market_raw.get("wwIdent"))
              and (market_name := market_raw.get("marketName"))
              and (street_with_house_number := market_raw.get("streetWithHouseNumber"))
              and (city := market_raw.get("city"))
              and (zip_code := market_raw.get("zipCode"))
              and (latitude := market_raw.get("latitude"))
              and (longitude := market_raw.get("longitude"))):
        Penny.LOGGER.warning("Problem with parsing the market: \
                       \nwwIdent=%s, marketName=%s, streetWithHouseNumber=%s, city=%s, zipCode=%s, latitude=%s, longitude=%s \
                       \noriginal data: %s", 
                       id, market_name, street_with_house_number, city, zip_code, latitude, longitude, market_raw)
        return None
      market = Market()
      market.id = market_raw["wwIdent"]
      market.type = Penny.TYPE
      market.name = market_raw["marketName"]
      market.address = market_raw["streetWithHouseNumber"]
      market.city = market_raw["city"]
      market.postal_code = market_raw["zipCode"]
      market.latitude = market_raw["latitude"]
      market.longitude = market_raw["longitude"]
      now = localtime()
      market.last_update = date(now.tm_year, now.tm_mon, now.tm_mday)
      markets.append(market)
    
    if extras:
      markets_extra = Penny.get_markets_extra(markets_raw)
      return markets, markets_extra
    else:
      return markets


class Rewe:

  TYPE = "rewe"
  LOGGER = None

  TMP_PATH_FOLDERNAME = "tmp"
  PRIVATE_KEY_FILENAME = "private.key"
  CERTIFICATE_FILENAME = "private.pem"
  SOURCE_PATH = Path(__file__).resolve().parent
  if not (os.path.exists(TMP_PATH := os.path.join(SOURCE_PATH, TMP_PATH_FOLDERNAME))):
    os.mkdir(TMP_PATH)
  FULL_KEY_FILE_PATH = os.path.join(TMP_PATH, PRIVATE_KEY_FILENAME)
  FULL_CERT_FILE_PATH = os.path.join(TMP_PATH, CERTIFICATE_FILENAME)

  @staticmethod
  @delay_range()
  def get_markets_extra(market: Market) -> Market_Extra:
    """add opening hours"""

    return market

  @staticmethod
  @delay_range()
  def _get_markets_from_zip_code(zip_code: str) -> list[Market]:
    files = os.listdir(Rewe.TMP_PATH)
    if Rewe.PRIVATE_KEY_FILENAME not in files or Rewe.CERTIFICATE_FILENAME not in files:
      possible_errors = get_rewe_creds(source_path=Rewe.TMP_PATH, key_filename=Rewe.PRIVATE_KEY_FILENAME, cert_filename=Rewe.CERTIFICATE_FILENAME)
      if possible_errors != None:
        Rewe.LOGGER.exception("Couldn't get the rewe credentials. %s", possible_errors)
        return "credential retrieving error"
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
    try:
      res = Client(http2=True, cert=(client_cert, client_key), headers=header).get(url)
    except:
      Rewe.LOGGER.warning("Problem with request to: %s", url)
      return None
    if res.status_code != 200:
      Rewe.LOGGER.warning("Unexpected status code %s from request to %s", res.status_code, url)
      return None
    try:
      res = res.json()
    except:
      Rewe.LOGGER.warning("Couldn't parse %s to a json", url)
      return None
    if (markets := res.get("markets")) == None:
      Rewe.LOGGER.warning("Property 'markets' not found in json from %s", url)
      return None
    if len(markets) == 0:
      return None
    for market_raw in markets:
      id = name = raw_values = postal_code = city = address_line1 = location = latitude = longitude = None
      if not ((id := market_raw.get("id"))
              and (name := market_raw.get("name"))
              and (raw_values := market_raw.get("rawValues"))
              and (postal_code := market_raw["rawValues"].get("postalCode"))
              and (city := market_raw["rawValues"].get("city"))
              and (address_line1 := market_raw.get("addressLine1"))
              and (location := market_raw.get("location"))
              and (latitude := market_raw["location"].get("latitude"))  # catches latitude = 0, ie. not valid
              and (longitude := market_raw["location"].get("longitude"))):  # catches longitude = 0, ie. not valid
        Rewe.LOGGER.warning("Problem with parsing the market \
                       \nid=%s, name=%s, rawValues=%s, postalCode=%s, city=%s, addressLine1=%s, location=%s, latitude=%s, longitude=%s \
                       \noriginal json: %s",
                       id, name, raw_values, postal_code, city, address_line1, location, latitude, longitude, market_raw)
        return None
      market = Market()
      market.id = market_raw["id"]
      market.type = Rewe.TYPE
      market.name = market_raw["name"]
      market.postal_code = market_raw["rawValues"]["postalCode"]
      market.city = market_raw["rawValues"]["city"]
      market.address = market_raw["addressLine1"]
      market.latitude = str(market_raw["location"]["latitude"])
      market.longitude = str(market_raw["location"]["longitude"])
      now = localtime()
      market.last_update = date(now.tm_year, now.tm_mon, now.tm_mday)
      all_markets.append(market)
    return all_markets

  @staticmethod
  def get_markets(zipcode_range: tuple[int, int] = (10000,99999), zipcodes: list[int] = None, *, loglevel: str = "DEBUG", one_logger_file: bool = True, logger_file_mode: str = "w") -> list[Market]:
    """zipcode_range takes precedence over zipcodes."""
    Rewe.LOGGER = setup_logger(SOURCE_PATH, __name__, Rewe.TYPE, loglevel, one_logger_file, logger_file_mode)

    all_markets: list[Market] = list()
    if zipcodes != None and isinstance(zipcodes, list) and all([isinstance(z, int) for z in zipcodes]):
      all_zipcodes = zipcodes
      Rewe.LOGGER.info("Using custom zipcodes list: %s", zipcodes)
    else:
      all_zipcodes = [i for i in range(*zipcode_range)]
      Rewe.LOGGER.info("Using zipcode range: %s", zipcode_range)
    while len(all_zipcodes) != 0:
      zipcode = random.choice(all_zipcodes)
      markets = Rewe._get_markets_from_zip_code(str(zipcode).rjust(5, "0"))
      if isinstance(markets, str):  # error
        return None
      elif markets != None:
        Rewe.LOGGER.info("Parsed %d markets for zipcode %s. Got markets: %s", len(markets), str(zipcode).rjust(5, "0"), [m.id for m in markets])
        for market in markets:
          # no doubles!
          if market.id not in [m.id for m in all_markets]:
            all_markets.append(market)
      all_zipcodes.remove(zipcode)
    Rewe.LOGGER.info("Returned %d markets for this batch.", len(all_markets))
    return all_markets


class Hit:
  """urls
  markets:
  https://www.hit.de/maerkte -> parsed.find("div", {"data-component": "store/marketfinder/marketfinder"}).get("data-stores") -> json.loads

  -> app
  header: {'user-agent': 'okhttp/4.12.0', 'x-api-key': 'live:android:cLpsXTCo5pR7URUu8gvSN9yJCPj5A'}
  
  https://www.hit.de/api/stores?fields=...
  https://www.hit.de/api/store/<storeId>?fields=...
  https://www.hit.de/api/stores/services  <- gets all possible services, like metzgerei/ Fleisch Bedienung
  https://www.hit.de/api/offers?&for_store=...&for_date=..&limit=1000  # optional: &is_featured=1
  https://www.hit.de/api/coupons

  fields:
  id,location,name,street,zip,city,phone,phone_url_formatted,services,technicalName,
  serviceType,logo,is_open,openings,images,mobile,desktop,flyers,isCurrentWeeklyFlyer,pdf,url
  
  """
  TYPE = "hit"
  LOGGER = None

  @staticmethod
  def get_markets(*, loglevel: str = "DEBUG", one_logger_file: bool = True, logger_file_mode: str = "w") -> list[Market]:
    Hit.LOGGER = setup_logger(SOURCE_PATH, __name__, Hit.TYPE, loglevel, one_logger_file, logger_file_mode)

    essential_fields = ["id", "location", "name", "street", "zip", "city"]
    url = f"https://www.hit.de/api/stores?fields={','.join(essential_fields)}&limit=1000"
    headers = {
      "user-agent": "okhttp/4.12.0",
      "x-api-key": "live:android:cLpsXTCo5pR7URUu8gvSN9yJCPj5A"
    }
    try:
      res = Client(http1=True, headers=headers, timeout=60.0).get(url)
    except:
      Hit.LOGGER.warning("Problem with request to: %s", url)
      return None
    if res.status_code != 200:
      Hit.LOGGER.warning("Unexpected status code %s from request to %s", res.status_code, url)
      return None
    markets: list[Market] = list()
    try:
      markets_raw = res.json()
    except:
      Hit.LOGGER.warning("Couldn't parse %s to json", url)
      return None
    if (markets_raw_data := markets_raw.get("data")) == None:
      Hit.LOGGER.warning("No data key in %s", url)
      return None
    for market_raw in markets_raw_data:
      id = location = latitude = longitude = name = street = zip = city = None
      if not ((id := market_raw.get("id"))
              and (location := market_raw.get("location"))
              and (latitude := location.get("latitude"))
              and (longitude := location.get("longitude"))
              and (name := market_raw.get("name"))
              and (street := market_raw.get("street"))
              and (zip := market_raw.get("zip"))
              and (city := market_raw.get("city"))):
        Hit.LOGGER.warning("Problem with parsing the market: \
                      \nid=%s, location=%s, latitude=%s, longitude=%s, name=%s, street=%s, zip=%s, city=%s \
                      \noriginal data: %s",
                      id, location, latitude, longitude, name, street, zip, city, market_raw)
        return None
      market = Market()
      market.type = Hit.TYPE
      market.id = id
      market.latitude = latitude
      market.longitude = longitude
      market.name = name
      market.address = street
      market.postal_code = zip
      market.city = city
      now = localtime()
      market.last_update = date(now.tm_year, now.tm_mon, now.tm_mday)
      markets.append(market)

    return markets
      

class Nahkauf:
  """urls
  https://www.nahkauf.de/.rest/nk/markets/list (for the markets)

  WITH cookie: nahkauf-markt=1762428;
  https://www.nahkauf.de/angebote-im-markt (offers)

  https://www.nahkauf.de/.rest/nk/markets/status  (opening status)
  """
  pass


if __name__ == "__main__":
  pass