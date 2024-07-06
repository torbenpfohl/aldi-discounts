import os
import re
import uuid
import json
import sqlite3
from pathlib import Path
from time import localtime, strftime
from datetime import date, timedelta

from httpx import Client
from bs4 import BeautifulSoup, element

from util import delay
from product import Product
from get_rewe_creds import get_rewe_creds

class Aldi_nord:
  
  @staticmethod
  @delay
  def _get_and_parse_product(url: str) -> Product:
    res = Client(http1=True).get(url)
    if res.status_code != 200:
      print("problem with the product link.")
      return None
    parsed = BeautifulSoup(res.text, "html.parser")
    new_product = Product()
    # describition
    desc_raw = parsed.find("script", {"type":"application/ld+json"}).string
    desc_raw = json.loads(desc_raw)
    desc = desc_raw.get("description")
    desc = re.sub(r"(\r|\n|\xa0)", "", desc)  # .replace("\xa0", "")
    new_product.description = desc
    # valid from
    valid_from_raw = parsed.find(lambda tag: tag.name == "div" and tag.has_attr("data-promotion-date-millis"))
    valid_from_timestamp = valid_from_raw.get("data-promotion-date-millis")
    valid_from = date.fromtimestamp(int(valid_from_timestamp[:-3]))
    new_product.valid_from = valid_from
    # valid to 
    # TODO: expect the end of the week (saturday)
    # producer
    producer_raw = parsed.find("span", class_="mod-article-tile__brand")
    producer = producer_raw.contents[0].strip()
    if producer != "":
      new_product.producer = producer
    # price
    price_raw = parsed.find("span", class_="price__wrapper")
    price = price_raw.contents[0].strip()
    new_product.price = price
    # price_before
    price_before_raw = parsed.find("s", class_="price__previous")
    if price_before_raw != None:
      price_before = price_before_raw.contents[0]
      new_product.price_before = price_before
    # ! price base
    price_base_raw = parsed.find("span", class_="price__base")
    if price_base_raw != None:
      ###print(price_base_raw.contents[0])
      pass
    # quantity/unit/amount
    quantity_raw = parsed.find("span", class_="price__unit")
    ###print(quantity_raw.contents[0])
    # name
    name_raw = parsed.find("span", class_="mod-article-tile__title")
    name = name_raw.contents[0].strip()
    new_product.name = name
    # link
    base_url = "https://www.aldi-nord.de"
    link_raw = parsed.find("a", class_="mod-article-tile__action")
    partial_link = link_raw.get("href")
    if partial_link.startswith("https"):
      # exclude discounts you only get in the online-shop
      return None
    link = base_url + partial_link
    new_product.link = link
    return new_product



  @staticmethod
  def get_products() -> list[Product]:
    all_products = list()
    url = "https://www.aldi-nord.de/angebote.html"
    res = Client(http1=True).get(url)
    if res.status_code != 200:
      print("unexpected status code.")
    parsed = BeautifulSoup(res.text, "html.parser")
    product_urls = list()
    products_raw = parsed.find_all(lambda x: x.name == "div" and x.has_attr("data-tile-url") and re.search(r".articletile.", x.get("data-tile-url")))
    if len(products_raw) == 0:
      print("no article tiles found.")
      return None
    base_url = "https://www.aldi-nord.de"
    for product_raw in products_raw:
      partial_url = product_raw.get("data-tile-url")
      product = Aldi_nord._get_and_parse_product(base_url + partial_url)
      if product != None:
        all_products.append(product)
      break
    return all_products


class Aldi_sued:
 
  @staticmethod
  def _get_urls_partial_week() -> list[str]:
    # TODO find a way to get only the urls for the current week  OR  if it's sunday the urls for the next week.
    now = localtime()
    weekday = now.tm_wday
    now_date = date(now.tm_year, now.tm_mon, now.tm_mday)
    if weekday == 6:  #sunday, use the next week
      week_start = now_date + timedelta(days=1)
      week_end = now_date + timedelta(days=6)
    else:
      week_start = now_date - timedelta(days=weekday)
      week_end = now_date + timedelta(days=(5-weekday))

    base_url = "https://www.aldi-sued.de"
    url = "https://www.aldi-sued.de/de/angebote.html"
    res = Client(http1=True).get(url)
    parsed = BeautifulSoup(res.text, "html.parser")
    urls = parsed.find("div", id="subMenu-1").find_all("a", href=re.compile(r"/de/angebote/d\.\d+-\d+-\d+\.html"))
    urls = [base_url + url.get("href") for url in urls]
    relevant_urls = list()
    for u in urls:
      date_in_url_raw = re.search(r"\d+-\d+-\d+", u).group()
      day, month, year = date_in_url_raw.split("-")
      date_in_url = date(int(year.strip()), int(month.strip()), int(day.strip()))
      if date_in_url >= week_start and date_in_url <= week_end:
        relevant_urls.append(u)
    return relevant_urls

  @staticmethod
  def _extract_products_whole_week(tag):
    """Used by BeautifulSoups find-function."""
    if tag.name != "figure":
      return False
    img = tag.find("img")
    if img == None:
      return False
    img_src = img.has_attr("data-src")
    figcaption = tag.find("figcaption")
    return img and img_src and figcaption
  
  @staticmethod
  def _parse_product_partial_week(product_raw: element.Tag, producers: list[str]) -> Product:
    new_product = Product()
    base_url = "https://www.aldi-sued.de"
    partial_link = product_raw.find("a").get("href")
    if partial_link.startswith("https"):
      # exclude discounts you only get in the online-shop
      return None
    link = base_url + partial_link
    new_product.link = link
    name = product_raw.find("a").find("div").find("h2").string
    new_product.name = name
    producer = None
    for prod in producers:
      if name.startswith(prod):
        producer = prod
        break
      elif name.startswith(prod.replace(" ", "")):
        producer = prod
        break
    new_product.producer = producer
    for span_tag in product_raw.find("a").find("div").find_all("span"):
      if span_tag.get("class") and "at-product-price_lbl" in span_tag.get("class") and "price" in span_tag.get("class"):
        price = span_tag.string.strip()
        new_product.price = price
      if span_tag.get("id") and "uvp-plp" in span_tag.get("id"):
        price_before = span_tag.string.strip()
        if price_before == "":
          price_before = None
        new_product.price_before = price_before
      if span_tag.get("class") and "additional-product-info" in span_tag.get("class"):
        desc = " ".join([i.strip() for i in span_tag.string.split("\n") if i.strip() != ""])
        new_product.description = desc
    valid_from = None
    for p_tag in product_raw.find("a").find("div").find_all("p"):
      if match := re.search(r"\d+\.\d+\.\d+", p_tag.string).group():
        valid_from = match
    if valid_from == None:  # When the date doesn't stand in the article-tile.
      date_header = product_raw.find_all_previous("h1", class_="plp_title")
      valid_from = re.search(r"\d+\.\d+\.\d+", str(date_header[0])).group()
    new_product.valid_from = valid_from
    # valid_to is assumed to be the end of the week (saturday)
    now = localtime()
    weekday = now.tm_wday
    valid_to = date(now.tm_year, now.tm_mon, now.tm_mday) + timedelta(days=(5 - weekday))
    valid_to = str(valid_to.day) + "." + str(valid_to.month) + "." + str(valid_to.year)
    new_product.valid_to = valid_to
    # print(valid_from, valid_to, name, price, price_before, producer, desc, link, sep=" | ")
    return new_product

  @staticmethod
  def _parse_product_whole_week(product_raw: element.Tag) -> Product:
    new_product = Product()
    # valid from/to
    current_year = localtime().tm_year
    for before in product_raw.previous_elements:  # TODO: try product_raw.find_all_previous(..)
      dates = re.findall(r"\d+\.\d+\.", str(before.string))  # what is standing here between the years?
      if len(dates) == 2:
        break
    valid_from = dates[0] + str(current_year)
    new_product.valid_from = valid_from
    valid_to = dates[1] + str(current_year)
    new_product.valid_to = valid_to
    
    # the rest
    name = product_raw.find("figcaption").find("h3").contents[0]
    new_product.name = name
    for index, p_tag in enumerate(product_raw.find("figcaption").find_all("p")):
      try:
        match index:
          case 2:
            price = p_tag.contents[0]
            new_product.price = price
            price_before = p_tag.find("span").find("s")
            if price_before != None:
              price_before = price_before.string
            new_product.price_before = price_before
          case 3:
            producer = p_tag.string
            if producer.strip() == "":
              producer = None
            new_product.producer = producer
          case 4:
            desc = "".join(p_tag.stripped_strings)
            new_product.description = desc
      except:
        return None  # in fruit & vegetable category the prices for the next week are not available before saturday 7 am
    # print(valid_from, valid_to, name, price, price_before, producer, desc, sep=" | ")
    return new_product

  @staticmethod
  @delay
  def _get_products_from_url_whole_week(url: str) -> list[Product]:
    res = Client(http1=True).get(url)
    parsed = BeautifulSoup(res.text, "html.parser")
    products = list()
    products_raw = parsed.find_all(Aldi_sued._extract_products_whole_week)
    for product_raw in products_raw:
      product = Aldi_sued._parse_product_whole_week(product_raw)
      if product == None:
        break
      products.append(product)
    return products
  
  @staticmethod
  @delay
  def _get_products_from_url_partial_week(url: str) -> list[Product]:
    res = Client(http1=True).get(url)
    parsed = BeautifulSoup(res.text, "html.parser")
    products = list()
    products_raw = parsed.find("div", id="plpProducts").find_all("article", class_="wrapper")
    producers = list()
    producers_raw = parsed.find("div", id="filter-list-brandName").find_all("label")
    for label_tag in producers_raw:
      producer = "".join(label_tag.stripped_strings)
      producer = re.search(r".+(?=\(\d+\))", producer).group()
      producer = producer.strip()
      producers.append(producer)
    for product_raw in products_raw:
      product = Aldi_sued._parse_product_partial_week(product_raw, producers)
      products.append(product)
    return products

  @staticmethod
  def get_products() -> list[Product]:
    urls_whole_week = [
      "https://www.aldi-sued.de/de/angebote/frischekracher.html", 
      "https://www.aldi-sued.de/de/angebote/preisaktion.html",
      "https://www.aldi-sued.de/de/angebote/markenaktion-der-woche.html"
      ]
    all_products = list()
    for url in urls_whole_week:
      products = Aldi_sued._get_products_from_url_whole_week(url)
      all_products.extend(products)

    urls_partial_week = Aldi_sued._get_urls_partial_week()
    for url in urls_partial_week:
      products = Aldi_sued._get_products_from_url_partial_week(url)
      all_products.extend(products)
    return all_products


class Penny:
  """
  https://www.penny.de/.rest/market

  https://www.penny.de/.rest/market/markets/market_230533

  https://www.penny.de/.rest/marketRegion/63302533

  https://www.penny.de/.rest/offers/2024-27
  
  https://www.penny.de/.rest/offers/2024-27?weekRegion=15A-05
  https://www.penny.de/.rest/offers/2024-27?weekRegion=15A-05&nextWeekRegion=15A-05-66
  
  https://www.penny.de/markt/darmstadt/230533/penny-pallas-pallaswiesenstr-70-72

  https://www.penny.de/.rest/recipes?path=/  # &filters=grillen-und-burger 
  <- filters are in the penny.de/clever-kochen/rezepte-und-ernaehrung html-code 
     (search for id starting with "recipe-filter-category-" and take the remaining string)
  -> penny.de/clever-kochen/rezepte-und-ernaehrung/rote-bete-salat-mit-patros-natur

  https://www.penny.de/.rest/recipes/ratings?slug=orientalischer-couscous-salat,wuerzige-hackspiesschen,...
  <- gets the ratings for the specified recipe(s)

  """

  @staticmethod
  def create_discount(offer_raw: dict):
    pass # TODO what to do with this?

  @staticmethod
  def store_selling_region_with_offer_ids(selling_region: str, offer_ids: list[str], last_update: str):
    pass

  @staticmethod
  def load_penny_selling_regions(market_db_path: str) -> list[str]:
    con = sqlite3.connect(market_db_path)
    cur = con.cursor()
    cur.execute("SELECT id FROM markets")
    selling_regions = cur.fetchall()
    selling_regions = [id[0] for id in selling_regions]
    selling_regions = list(set(selling_regions))
    cur.close()
    con.close()
    return selling_regions

  @staticmethod
  @delay
  def get_products_with_selling_region(selling_region: str) -> tuple[list[Product], list[str]]:
    all_offers = list()
    all_offer_ids = list()
    now = localtime()
    day, month, year = now.tm_mday, now.tm_mon, now.tm_year
    now_date = date(year, month, day)
    calendar_week = now_date.isocalendar().week
    base_url_penny = "https://www.penny.de"
    base_url = f"https://www.penny.de/.rest/offers/{year}-{calendar_week}"
    url = base_url + "?weekRegion=" + selling_region
    res = Client(http1=True, timeout=20.0).get(url)
    print(res.status_code)
    if res.status_code != 200:
      print("something wrong with penny market list request, url: ", url)
      return None
    markets = res.json()
    # groundwork for valid_from and valid_to
    weekstart = now_date - timedelta(days=now.tm_wday)
    periods = dict()
    for period in markets[0]["categoriesMenuPeriod"].values():
      # e.g. {"ab-montag": (datetime.date(2024, 7, 1), datetime.date(2024, 7, 6)), ...}
      periods[period["slug"]] = (weekstart + timedelta(days=period["startDayIndex"]), weekstart + timedelta(days=period["endDayIndex"]))
    # loop through categories
    for category in markets[0]["categories"]:
      # ignore payback offers
      if re.search(r".*payback.*", category["name"]):
        continue
      # set valid_from and valid_to
      for date_pattern, dates in periods.items():
        if category["id"].startswith(date_pattern):
          valid_from = dates[0]
          valid_to = dates[1]
          break
      # loop through offers
      offers = category["offerTiles"]
      for offer_raw in offers:
        product = Product()
        if offer_raw.get("title") == None:
          continue  # ignore marketing tiles
        if offer_raw.get("onlyOnline"):
          continue  # ignore online offers
        if re.search(r"rabatt", offer_raw["price"], re.IGNORECASE):
          Penny.create_discount(offer_raw)  # create discount objects ("RABATT"-tiles)
          continue
        product.name = offer_raw["title"].replace("\xad", "").strip().removesuffix("*")
        product.price = offer_raw["price"].removesuffix("*")
        product.valid_from = valid_from
        product.valid_to = valid_to
        product.unique_id = offer_raw["uuid"]
        product.link = base_url_penny + offer_raw["detailLinkHref"]
        if (original_price := offer_raw.get("originalPrice")) != None:
          product.price_before = original_price
        if offer_raw.get("showOnlyWithTheAppBadge"):
          product.app_deal = True
        else:
          product.app_deal = False
        if quantity := offer_raw.get("quantity"):
          product.quantity = quantity
        if price_base := offer_raw.get("basePrice"):
          # divide offer_raw["basePrice"] into product.base_price and product.base_price_unit
          base_price_unit, base_price = price_base.strip("()").split("=")
          product.base_price_unit = base_price_unit.removeprefix("1").strip()
          product.base_price = base_price.strip()
        if offer_raw.get("subtitle"):  # e.g. "je 250 g (1 kg = 1.76)", "je Packung",
          # check for base_price and base_price_unit
          if re.search(r".*\(.*=.*\).*", offer_raw["subtitle"]):
            quantity_raw, base_price_raw = offer_raw["subtitle"].removesuffix(")").split("(")
            product.quantity = quantity_raw.strip()
            base_price_unit, base_price = base_price_raw.split("=")
            product.base_price_unit = base_price_unit.removeprefix("1").strip()
            product.base_price = base_price.strip()
          else:  # only quantity in "subtitle"
            product.quantity = offer_raw["subtitle"]
        all_offers.append(product)
        all_offer_ids.append(product.unique_id)

    return all_offers, all_offer_ids
  
  @staticmethod
  @delay
  def get_product_details(product: Product) -> Product:
    """Call the product.link (gets some product details) add description and change the link."""
    res = Client(http2=True).get(product.link)
    if res.status_code != 200:
      print("something wrong with penny offer details, url: ", product.link)
      return product
    parsed = BeautifulSoup(res.text, "html.parser")
    details = parsed.find("div", class_="detail-block__body")
    if details != None:
      details = [detail for detail in details.find_all(string=True) if detail != "\n" and detail != "\xa0"]
      if len(details) != 0:
        details = ", ".join(details)
        product.description = details.replace("\xa0", "")
    product.link = product.link.removesuffix("~mgnlArea=main~")

    return product

  @staticmethod
  def get_products(market_db_path: str) -> list[Product]:
    now = localtime()
    last_update = str(now.tm_year) + "-" + str(now.tm_yday)
    selling_regions = Penny.load_penny_selling_regions(market_db_path)
    all_offers: set[Product] = set()
    all_offer_ids: set[str] = set()
    for selling_region in selling_regions:
      selling_region_offers, selling_region_offer_ids = Penny.get_products_with_selling_region(selling_region)
      all_offers.update(set(selling_region_offers))
      all_offer_ids.update(set(selling_region_offer_ids))
      Penny.store_selling_region_with_offer_ids(selling_region, list(set(selling_region_offer_ids)), last_update)
    all_offers = list(all_offers)
    all_offers = map(Penny.get_product_details, all_offers)
    return all_offers


class Rewe:
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
  pass