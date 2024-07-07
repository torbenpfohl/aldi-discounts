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

from util import delay, get_rewe_creds
from product import Product

# TODO: make sure BeautifulSoup can be GCed -> str(...)

class Aldi_nord:
  
  @staticmethod
  @delay
  def _get_and_parse_product(url: str) -> Product:
    res = Client(http1=True).get(url)
    if res.status_code != 200:
      print("problem with the product link.")
      return None
    parsed = BeautifulSoup(res.text, "html.parser")
    product = Product()
    # descripition
    desc_raw = parsed.find("script", {"type":"application/ld+json"}).string
    desc_raw = json.loads(desc_raw)
    desc = desc_raw.get("description")
    desc = desc.replace("\xa0", "").replace("\r", "").removesuffix("\n").replace("\n", " ")
    product.description = desc
    # add possible "price__info" (e.g. with 'Abtropfgewicht ...)
    # TODO: maybe move this extra_info into a seperate property?
    if extra_info := parsed.find("span", class_="price__info"):
      if product.description == None or product.description == "":
        product.description = extra_info.contents[0]
      else:
        product.description = extra_info.contents[0] + ", " + product.description
    # valid from
    valid_from_raw = parsed.find(lambda tag: tag.name == "div" and tag.has_attr("data-promotion-date-millis"))
    valid_from_timestamp = valid_from_raw.get("data-promotion-date-millis")
    valid_from = date.fromtimestamp(int(valid_from_timestamp[:-3]))
    product.valid_from = valid_from
    # valid to, assume the end of the week
    product.valid_to = valid_from + timedelta(days=(5 - valid_from.weekday()))
    # producer
    producer_raw = parsed.find("span", class_="mod-article-tile__brand")
    producer = producer_raw.contents[0].strip()
    if producer != "":
      product.producer = producer
    # price
    price_raw = parsed.find("span", class_="price__wrapper")
    price = price_raw.contents[0].strip()
    product.price = price
    # price_before
    price_before_raw = parsed.find("s", class_="price__previous")
    if price_before_raw != None:
      price_before = price_before_raw.contents[0]
      product.price_before = price_before
    # base_price and base_price_unit
    price_base_raw = parsed.find("span", class_="price__base")
    if price_base_raw != None:
      base_price_unit, base_price = price_base_raw.contents[0].split("=")
      product.base_price_unit = base_price_unit.strip()
      product.base_price = base_price.strip()
    # quantity
    quantity_raw = parsed.find("span", class_="price__unit")
    product.quantity = quantity_raw.contents[0]
    # name
    name_raw = parsed.find("span", class_="mod-article-tile__title")
    name = name_raw.contents[0].strip()
    product.name = name
    # link
    base_url = "https://www.aldi-nord.de"
    link_raw = parsed.find("a", class_="mod-article-tile__action")
    partial_link = link_raw.get("href")
    if partial_link.startswith("https"):
      # exclude discounts you only get in the online-shop
      return None
    link = base_url + partial_link
    product.link = link
    # unique_id
    product.unique_id = link_raw.get("data-attr-prodid")

    return product

  @staticmethod
  @delay
  def get_product_details(product: Product) -> Product:
    """call the link and add the description"""
    res = Client(http2=True, timeout=20).get(product.link)
    if res.status_code != 200:
      print("problem with aldi nord get_product_details, url: ", product.link)
    parsed = BeautifulSoup(res.text, "html.parser")
    desc_wrapper_raw = parsed.select("div.mod.mod-copy")[0]
    if li_tags := desc_wrapper_raw.find_all("li"):
      desc = list()
      for li_tag in li_tags:
        d = "".join([text for text in li_tag.find_all(string=True) if text != "\n"])
        desc.append(d)
      desc = ", ".join(desc)
      if product.description != None:
        product.description = product.description + " | " + desc
      else:
        product.description = desc

    return product

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
        product = Aldi_nord.get_product_details(product)
        all_products.append(product)

    return all_products


class Aldi_sued:
 
  @staticmethod
  @delay
  def _partial_week_get_product_details(product: Product) -> Product:
    """Calls product.link and extracts more description."""
    if product.link == "":
      return product
    res = Client(http2=True).get(product.link)
    if res.status_code != 200:
      print("Problem with aldi sued _get_product_details_..., url: ", product.link)
      return None
    parsed = BeautifulSoup(res.text, "html.parser")
    infoboxes = parsed.find_all("div", class_="infobox")
    infos = list()
    for infobox in infoboxes:
      infobox_infos = infobox.find_all(string=True)
      infos.extend(infobox_infos)
    infos = [info.strip(" \n\t\xa0") for info in infos]
    infos = [info for info in infos if info != "" if info != "Zum Garantieportal"]
    infos = ", ".join(infos)
    product.description = infos

    return product

  @staticmethod
  def _partial_week_get_urls() -> tuple[list[str], list[date]]:
    # get only the urls for the current week  OR  if it's sunday the urls for the next week.
    now = localtime()
    weekday = now.tm_wday
    now_date = date(now.tm_year, now.tm_mon, now.tm_mday)
    if weekday == 6:  #sunday, use the next week
      week_start = now_date + timedelta(days=1)
      week_end = now_date + timedelta(days=6)
    else:
      week_start = now_date - timedelta(days=weekday)
      week_end = now_date + timedelta(days=(5 - weekday))

    base_url = "https://www.aldi-sued.de"
    url = "https://www.aldi-sued.de/de/angebote.html"
    res = Client(http1=True).get(url)
    if res.status_code != 200:
      print("Problem with aldi sued get urls for partial week requests, url: ", url)
      return None
    parsed = BeautifulSoup(res.text, "html.parser")
    urls = parsed.find("div", id="subMenu-1").find_all("a", href=re.compile(r"/de/angebote/d\.\d+-\d+-\d+\.html"))
    urls = [base_url + url.get("href") for url in urls]
    relevant_urls = list()
    url_dates = list()
    for u in urls:
      date_in_url_raw = re.search(r"\d+-\d+-\d+", u).group()
      day, month, year = date_in_url_raw.split("-")
      url_date = date(int(year.strip()), int(month.strip()), int(day.strip()))
      if url_date >= week_start and url_date <= week_end:
        relevant_urls.append(u)
        url_dates.append(url_date)
    # normally we have Montag-, Donnerstag-, Freitag- and Samstag-URLs
    # if we don't get whose (e.g. at the end of the week) we create the urls ourselves
    if len(relevant_urls) < 4:
      relevant_urls = list()
      url_dates = list()
      days = [0, 3, 4, 5]
      for d in days:
        url_date = week_start + timedelta(days=d)
        day = str(url_date.day).rjust(2, "0")
        month = str(url_date.month).rjust(2, "0")
        year = str(url_date.year)
        url_pattern = f"https://www.aldi-sued.de/de/angebote/d.{day}-{month}-{year}.html"
        relevant_urls.append(url_pattern)
        url_dates.append(url_date)

    return relevant_urls, url_dates

  @staticmethod
  def _whole_week_bs4_extract_products(tag):
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
  def _partial_week_parse_product(product_raw: element.Tag, producers: list[str], url_date: date) -> Product:
    product = Product()
    base_url = "https://www.aldi-sued.de"
    # link
    partial_link = product_raw.find("a").get("href")
    if partial_link.startswith("https"):
      # exclude discounts you only get in the online-shop
      return None
    link = base_url + partial_link
    product.link = link
    # name
    name = product_raw.find("a").find("div").find("h2").string
    product.name = name
    # producer
    producer = None
    for prod in producers:
      if name.startswith(prod):
        producer = prod
        break
      elif name.startswith(prod.replace(" ", "")):
        producer = prod
        break
    product.producer = producer
    # price, price_before, description
    for span_tag in product_raw.find("a").find("div").find_all("span"):
      if span_tag.get("class") and "at-product-price_lbl" in span_tag.get("class") and "price" in span_tag.get("class"):
        price = span_tag.string.strip()
        product.price = price.removeprefix("€").strip()
      if span_tag.get("id") and "uvp-plp" in span_tag.get("id"):
        price_before = span_tag.string.strip()
        if price_before == "":
          price_before = None
        product.price_before = price_before
      if span_tag.get("class") and "additional-product-info" in span_tag.get("class"):
        desc = " ".join([i.strip() for i in span_tag.string.split("\n") if i.strip() != ""])
        product.description = desc
    # extract from description quantity, base_price, base_price_unit (if available)
    if product.description != "":
      description = product.description
      if re.search(r".*\(.*=.*\)", description):
        quantity, base_price_raw = description.removesuffix(")").split("(")
        product.quantity = quantity.strip()
        base_price, base_price_unit = base_price_raw.split("=")
        product.base_price = base_price.strip().removeprefix("1").strip()
        product.base_price_unit = base_price_unit.strip().removeprefix("€").strip()
      else:
        product.quantity = description
    product.description = None
    # valid_from
    product.valid_from = url_date
    # valid_to is assumed to be the end of the week (saturday)
    product.valid_to = url_date + timedelta(days=(5 - url_date.weekday()))
    # unique_id
    unique_id = re.search(r".+?(\d+).html", product.link)
    product.unique_id = unique_id

    return product

  @staticmethod
  @delay
  def _partial_week_get_products_from_url(url: str, url_date: date) -> list[Product]:
    res = Client(http1=True).get(url)
    parsed = BeautifulSoup(res.text, "html.parser")
    producers = list()
    producers_raw = parsed.find(id="filter-list-brandName")
    if producers_raw != None:
      producers_raw = producers_raw.find_all("label")
      for label_tag in producers_raw:
        producer = "".join(label_tag.stripped_strings)
        producer = re.search(r".+(?=\(\d+\))", producer).group()
        producer = producer.strip()
        producers.append(producer)
    products = list()
    products_raw = parsed.find("div", id="plpProducts").find_all("article", class_="wrapper")
    for product_raw in products_raw:
      product = Aldi_sued._partial_week_parse_product(product_raw, producers, url_date)
      product = Aldi_sued._partial_week_get_product_details(product)
      products.append(product)

    return products


  @staticmethod
  def _whole_week_parse_product(product_raw: element.Tag) -> Product:
    """Parse raw html product to Product."""
    product = Product()
    # valid_from and valid_to
    now = localtime()
    for before in product_raw.previous_elements:  # TODO: try product_raw.find_all_previous(..)
      dates = re.findall(r"\d+\.\d+\.", str(before.string))  # TODO: what is standing here between the years?
      if len(dates) == 2:
        break
    valid_from_day, valid_from_month = dates[0].removesuffix(".").split(".")
    valid_from = date(now.tm_year, int(valid_from_month), int(valid_from_day))
    product.valid_from = valid_from
    valid_to_day, valid_to_month = dates[1].removesuffix(".").split(".")
    valid_to = date(now.tm_year, int(valid_to_month), int(valid_to_day))
    product.valid_to = valid_to
    # from monday to saturday only get the products of the current week.
    # on sunday we can get the products of the next week
    now_date = date(now.tm_year, now.tm_mon, now.tm_mday)
    if now.tm_wday == 6:  # sunday
      week_start = now_date + timedelta(days=1)
      week_end = now_date + timedelta(days=6)
    else:
      week_start = now_date - timedelta(days=now.tm_wday)
      week_end = now_date + timedelta(days=(5 - now.tm_wday))
    if valid_from < week_start or valid_from > week_end:
      return None

    # the other properties
    name = product_raw.find("figcaption").find("h3").contents[0]
    product.name = name
    p_tags = product_raw.find("figcaption").find_all("p")
    for index, p_tag in enumerate(p_tags):
      # with 4 p-tags we have no producer field. and the description is in the producer field
      try:
        if len(p_tags) == 5:
          match index:
            case 2:
              price = p_tag.contents[0]
              product.price = price.strip().removeprefix("€").removesuffix("*").strip()
              price_before = p_tag.find("span").find("s")
              if price_before != None:
                price_before = price_before.string
              product.price_before = price_before
            case 3:
              producer = p_tag.string
              if producer.strip() == "":
                producer = None
              product.producer = producer
            case 4:
              desc = "".join(p_tag.stripped_strings)
              product.description = desc
        elif len(p_tags) == 4:  # no producer field.
          match index:
            case 2:  # price and maybe price_before
              price = p_tag.contents[0]
              product.price = price.strip().removeprefix("€").removesuffix("*").strip()
              price_before = p_tag.find("span").find("s")
              if price_before != None:
                price_before = price_before.string
              product.price_before = price_before
            case 3:
              desc = "".join(p_tag.stripped_strings)
              product.description = desc
      except:
        return None  # in fruit & vegetable category the prices for the next week are not available before saturday 7 am
      
    # TODO: parse product.description: quantity, base_price, base_price_unit are all in there
    #       also with vegetables and fruits you get the origin

    return product

  @staticmethod
  @delay
  def _whole_week_get_products_from_url(url: str) -> list[Product]:
    res = Client(http1=True).get(url)
    parsed = BeautifulSoup(res.text, "html.parser")
    products = list()
    products_raw = parsed.find_all(Aldi_sued._whole_week_bs4_extract_products)
    for product_raw in products_raw:
      product = Aldi_sued._whole_week_parse_product(product_raw)
      if product != None:
        products.append(product)
    return products


  @staticmethod
  def get_products() -> list[Product]:
    all_products = list()
    urls_whole_week = [
      "https://www.aldi-sued.de/de/angebote/frischekracher.html", 
      "https://www.aldi-sued.de/de/angebote/preisaktion.html",
      "https://www.aldi-sued.de/de/angebote/markenaktion-der-woche.html"
      ]
    for url in urls_whole_week:
      products = Aldi_sued._whole_week_get_products_from_url(url)
      all_products.extend(products)

    urls_partial_week, url_dates = Aldi_sued._partial_week_get_urls()
    for url, url_date in zip(urls_partial_week, url_dates):
      products = Aldi_sued._partial_week_get_products_from_url(url, url_date)
      all_products.extend(products)
    return all_products


class Penny:
  """"""
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

  COLUMNS = ["selling_region", "offer_ids", "last_update"]
  SELLING_REGION_TABLE = "selling_region_with_offer_ids"

  @staticmethod
  def create_vouchers(offer_raw: dict):
    pass # TODO what to do with this?

  @staticmethod
  def store_selling_region_with_offer_ids(selling_regions_db_path: str, selling_region: str, offer_ids: list[str], last_update: str):
    """Append existing database. Create if not exists."""
    offer_ids.sort(key=lambda id: uuid.UUID(id))
    con = sqlite3.connect(selling_regions_db_path)
    cur = con.cursor()
    cur.execute(f"CREATE TABLE if not exists {Penny.SELLING_REGION_TABLE}({','.join(Penny.COLUMNS)})")
    cur.execute(f"INSERT INTO {Penny.SELLING_REGION_TABLE}({','.join(Penny.COLUMNS)}) VALUES('{selling_region}', '{','.join(offer_ids)}', '{last_update}')")
    con.commit()
    cur.close()
    con.close()

  @staticmethod
  def load_penny_selling_regions(market_db_path: str) -> list[str]:
    if not os.path.exists(market_db_path):
      print("penny database-file for selling regions doesn't exist. bad path: ", market_db_path)
      return None
    con = sqlite3.connect(market_db_path)
    cur = con.cursor()
    cur.execute("SELECT id FROM markets where type='penny'")
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
          Penny.create_vouchers(offer_raw)  # create discount objects ("RABATT"-tiles)
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
  def get_products(market_db_path: str, selling_regions_db_path: str) -> list[Product]:
    now = localtime()
    last_update = str(now.tm_year) + "-" + str(now.tm_yday)
    selling_regions = Penny.load_penny_selling_regions(market_db_path)
    all_offers: set[Product] = set()
    all_offer_ids: set[str] = set()
    for selling_region in selling_regions:
      selling_region_offers, selling_region_offer_ids = Penny.get_products_with_selling_region(selling_region)
      all_offers.update(set(selling_region_offers))
      all_offer_ids.update(set(selling_region_offer_ids))
      Penny.store_selling_region_with_offer_ids(selling_regions_db_path, selling_region, list(set(selling_region_offer_ids)), last_update)
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
  MARKET_ID_TABLE = "market_id_with_offer_ids"

  PRIVATE_KEY_FILENAME = "private.key"
  CERTIFICATE_FILENAME = "private.pem"
  SOURCE_PATH = Path(__file__).resolve().parent
  FULL_KEY_FILE_PATH = os.path.join(SOURCE_PATH, PRIVATE_KEY_FILENAME)
  FULL_CERT_FILE_PATH = os.path.join(SOURCE_PATH, CERTIFICATE_FILENAME)

  @staticmethod
  def store_market_with_offer_ids(market_ids_db_path: str, market_id: str, offer_ids: list[str], last_update: str):
    offer_ids.sort(key=lambda id: int(id))
    con = sqlite3.connect(market_ids_db_path)
    cur = con.cursor()
    cur.execute(f"CREATE TABLE if not exists {Rewe.MARKET_ID_TABLE}({','.join(Rewe.COLUMNS)})")
    cur.execute(f"INSERT INTO {Rewe.MARKET_ID_TABLE}({','.join(Rewe.COLUMNS)}) VALUES('{market_id}', '{','.join(offer_ids)}', '{last_update}')")
    con.commit()
    cur.close()
    con.close()

  @staticmethod
  def load_rewe_market_ids(market_db_path: str) -> list[str]:
    # load from database
    con = sqlite3.connect(market_db_path)
    cur = con.cursor()
    cur.execute("SELECT market_id FROM markets where type='rewe'")
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
      print("problem with the rewe stationary offer api.")
      return None
    res = res.json()
    raw_products = res["data"]["offers"]
    all_offers = list()
    all_offer_ids = list()
    # valid_to
    valid_to = localtime(raw_products["untilDate"] / 1000)
    valid_to = date(valid_to.tm_year, valid_to.tm_mon, valid_to.tm_mday)
    for category in raw_products["categories"]:
      if re.search(r".*PAYBACK.*", category["title"]):
        continue
      category_offers = category["offers"]
      for offer in category_offers:
        if offer["title"] == "":
          continue
        product = Product()
        # name
        product.name = offer["title"]
        # valid_to
        product.valid_to = valid_to
        # price
        product.price = offer["priceData"]["price"].removesuffix("€").strip()
        # description
        product.description = offer["subtitle"]
        # extract quantity, base_price, base_price_unit from offer["subtitle"]
        if base_price_raw := re.search(r".*?(\(.*?=.*?\)).*", product.description):
          base_price_raw = base_price_raw.group(1)
          product.description = product.description.replace(base_price_raw, "")
          base_price_unit, base_price = base_price_raw.split("=")
          product.base_price = base_price.removesuffix(")").strip()
          product.base_price_unit = base_price_unit.removeprefix("(1").strip()
        # detail.contents has a Art.-Nr. and can have a Hersteller and sometimes also a Herkunft
        for line in offer["detail"]["contents"]:
          if line["header"] == "Produktdetails":
            details = line["titles"]
        for line in details:
          if line.startswith("Art.-Nr."):
            # unique_id
            product.unique_id = line.removeprefix("Art.-Nr.:").strip()
          if line.startswith("Hersteller"):
            # producer
            product.producer = line.removeprefix("Hersteller:").strip()
          if line.startswith("Herkunft"):
            # origin
            product.origin = line.removeprefix("Herkunft:").strip()
        # quantity
        pattern = r".*?(je.*?)(( , ).*|(, ).*|(\. ).*|\Z)"
        quantity = re.search(pattern, product.description)
        if quantity:
          quantity = quantity.group(1)
          product.quantity = quantity
          product.description = product.description.replace(quantity, "")
          # clean up the product.description
          product.description = product.description.strip(" .,").replace(" , ", "").replace(" .", "")
          if product.description == "":
            product.description = None

        # search for "Pfand" -> pattern: r"zzgl. (.*?) Pfand"  # (\d+\.\d{2})
        
        # offer["detail"]["nutriScore"]
        # pitchIn (extra description)
        if (detail := offer.get("detail")) and (pitch_in := detail.get("pitchIn")):
            if product.description:
              product.description += ", " + pitch_in
            else:
              product.description = pitch_in
        # valid_to <- assumed to be the start of the week
        valid_to_weekday = product.valid_to.weekday()
        valid_from = valid_to - timedelta(days=valid_to_weekday)
        product.valid_from = valid_from

        # price_before, link <- missing
        all_offers.append(product)
        all_offer_ids.append(product.unique_id)

    return all_offers, all_offer_ids


  @staticmethod
  @delay
  def get_product_details(product: Product) -> Product:
    # TODO: call different rewe api
    # write into different database _> general db for products with a special 
    #   column for rewe_article_id and listing_id (for adding to basket)
    # contains ingredients, contact_info, gtin/ean, current_retail_price, nutrition_facts, ...


    return product
    

  @staticmethod
  def get_products(market_db_path: str, market_ids_db_path: str) -> list[Product]:
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
      Rewe.store_market_with_offer_ids(market_ids_db_path, market_id, list(set(market_offer_ids)), last_update)
    #all_offers = list(map(Rewe.get_product_details, all_offers))
    all_offers = list(all_offers)
    return all_offers


if __name__ == "__main__":
  ps, ps_ids = Rewe.get_products_with_market_id("565950")
  print(len(ps), len(ps_ids))
  print(ps_ids)
  
  pass