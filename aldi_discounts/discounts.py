import os
import re
import uuid
import json
import logging
from copy import copy
from pathlib import Path
from time import localtime
from datetime import date, timedelta, datetime, timezone

from httpx import Client
from bs4 import BeautifulSoup, element

from product import Product, is_valid_product, cust_hash
from product import store as store_product
from market import get_market_ids
from market_products import Market_Products
from market_products import get_all_ids as get_market_ids_market_products
from market_products import store_multiple as store_multiple_market_products
from util import delay_range, get_rewe_creds, set_week_start_and_end, setup_logger

__all__ = ["Aldi_sued", "Aldi_nord", "Hit", "Penny", "Rewe", "Netto"]

LOG_PATH_FOLDERNAME = "log"
DATA_PATH_FOLDERNAME = "data"
SOURCE_PATH = Path(__file__).resolve().parent
if not (os.path.exists(DATA_PATH := os.path.join(SOURCE_PATH, DATA_PATH_FOLDERNAME))):
  os.mkdir(DATA_PATH)
if not (os.path.exists(LOG_PATH := os.path.join(SOURCE_PATH, LOG_PATH_FOLDERNAME))):
  os.mkdir(LOG_PATH)

logger = logging.getLogger(__name__)
log_handler = logging.FileHandler(os.path.join(LOG_PATH, f"{__name__}.log"), mode="w", encoding="utf-8")
log_formatter = logging.Formatter("%(name)s (%(levelname)s) - %(asctime)s - %(message)s")
log_handler.setFormatter(log_formatter)
logger.addHandler(log_handler)
logger.setLevel("DEBUG") # normally this is set from the importing module




# TODO after that: implement progressbar (and add logging whereever useful)

class Aldi_nord:
  
  TYPE = "aldi_nord"
  
  @staticmethod
  @delay_range()
  def _get_and_parse_product(url: str) -> Product:
    try:
      res = Client(http1=True).get(url)
    except:
      logger.warning("Aldi_nord: Problem with request to %s", url)
      return None
    if res.status_code != 200:
      logger.warning("Aldi_nord: unexpected status code %s from request to %s", res.status_code, url)
      return None
    parsed = BeautifulSoup(res.text, "html.parser")
    product = Product()
    product.market_type = Aldi_nord.TYPE

    # descripition
    desc_raw = parsed.find("script", {"type":"application/ld+json"})
    if desc_raw != None:
      desc_raw = desc_raw.string
      try:
        desc_raw = json.loads(desc_raw)
        if desc := desc_raw.get("description"):
          desc = desc.replace("\xa0", "").replace("\r", "").removesuffix("\n").replace("\n", " ")
          if desc != "":
            product.description = desc
        else:
          logger.debug("Aldi_nord: Could not get description attribute in %s", url)
      except:
        logger.debug("Aldi_nord: Could not load description-json for %s", url)
    else:
      logger.debug("Aldi_nord: No description object for %s", url)

    # add possible "price__info" (e.g. with 'Abtropfgewicht ...)
    # TODO: maybe move this extra_info into a seperate property?
    if (extra_info := parsed.find("span", class_="price__info")) and extra_info.contents and len(extra_info.contents) != 0:
      if product.description == None or product.description == "":
        product.description = str(extra_info.contents[0])
      else:
        product.description = str(extra_info.contents[0]) + ", " + product.description

    # valid from
    valid_from_raw = parsed.find(lambda tag: tag.name == "div" and tag.has_attr("data-promotion-date-millis"))
    if valid_from_raw != None:
      if valid_from_timestamp := valid_from_raw.get("data-promotion-date-millis"):
        try:
          timestamp = int(valid_from_timestamp[:-3])
          valid_from = date.fromtimestamp(timestamp)
          product.valid_from = valid_from
          # valid to, assume the end of the week, i.e. saturday
          product.valid_to = valid_from + timedelta(days=(5 - valid_from.weekday()))
        except:
          logger.debug("Aldi_nord: Couldn't parse timestamp for %s", url)
      else:
        logger.debug("Aldi_nord: No date timestamp for %s", url)
    else:
      logger.debug("Aldi_nord: No date object for %s", url)

    # producer
    producer_raw = parsed.find("span", class_="mod-article-tile__brand")
    if producer_raw != None and producer_raw.contents != None and len(producer_raw.contents) != 0:
      producer = producer_raw.contents[0].strip()
      if producer != "":
        product.producer = str(producer)
      else:
        logger.debug("Aldi_nord: No producer for %s", url)
    else:
      logger.debug("Aldi_nord: No producer object for %s", url)

    # price
    price_raw = parsed.find("span", class_="price__wrapper")
    if price_raw != None and price_raw.contents != None and len(price_raw.contents) != 0:
      price = price_raw.contents[0].strip()
      if price != "":
        product.price = str(price)
      else:
        logger.debug("Aldi_nord: No price for %s", url)
    else:
      logger.debug("Aldi_nord: No price object for %s", url)
    
    # price_before
    price_before_raw = parsed.find("s", class_="price__previous")
    if price_before_raw != None and price_before_raw.contents != None and len(price_before_raw.contents) != 0:
      price_before = price_before_raw.contents[0].strip()
      if price_before != "":
        product.price_before = str(price_before)
      else:
        logger.debug("Aldi_nord: No price before for %s", url)
    else:
      logger.debug("Aldi_nord: No price before object for %s", url)

    # base_price and base_price_unit
    price_base_raw = parsed.find("span", class_="price__base")
    if price_base_raw != None and price_base_raw.contents != None and len(price_base_raw.contents) != 0:
      base_price_unit_raw, base_price_raw = price_base_raw.contents[0].split("=")
      if isinstance(base_price_unit_raw, str) and ((base_price_unit := base_price_unit_raw.strip()) != ""):
        product.base_price_unit = base_price_unit
      else:
        logger.debug("Aldi_nord: No base price unit for %s", url)
      if isinstance(base_price_raw, str) and ((base_price := base_price_raw.strip()) != ""):
        product.base_price = base_price
      else:
        logger.debug("Aldi_nord: No base price for %s", url)
    else:
      logger.debug("Aldi_nord: No base price unit and base price for %s", url)

    # quantity
    quantity_raw = parsed.find("span", class_="price__unit")
    if quantity_raw != None and quantity_raw.contents != None and len(quantity_raw.contents) != 0:
      quantity = quantity_raw.contents[0].strip()
      if quantity != "":
        product.quantity = quantity
      else:
        logger.debug("Aldi_nord: No quantity for %s", url)
    else:
      logger.debug("Aldi_nord: No quantity object for %s", url)

    # name
    name_raw = parsed.find("span", class_="mod-article-tile__title")
    if name_raw != None and name_raw.contents != None and len(name_raw.contents) != 0:
      name = name_raw.contents[0].strip()
      if name != "":
        product.name = str(name)
      else:
        logger.debug("Aldi_nord: No name for %s", url)
    else:
      logger.debug("Aldi_nord: No name object for %s", url)
    
    # link
    base_url = "https://www.aldi-nord.de"
    link_raw = parsed.find("a", class_="mod-article-tile__action")
    if link_raw != None and (partial_link := link_raw.get("href")):
      # exclude discounts you only get in the online-shop
      if partial_link.startswith("https"):
        return None
      link = base_url + partial_link
      product.link = link
      # unique_id
      if uid := link_raw.get("data-attr-prodid"):
        product.unique_id = str(uid)
        product.unique_id_internal = product.unique_id
      else:
        logger.debug("Aldi_nord: No unique id for %s", url)
    else:
      logger.debug("Aldi_nord: No link object for %s", url)

    # TODO: ask for the essential product properties (in an extra function to make this a general rule for all APIs)
    #       if there not meet, set product to None, i.e. return None
    # product = is_valid_product(product)
    # if product == None:
    #   logger.warning("Aldi_nord: %s did not meet property requirements.", url)
    return product

  @staticmethod
  @delay_range()
  def get_product_details(product: Product) -> Product:
    """Call the link and add the description."""
    if product.link == None or product.link == "":
      logger.debug("Aldi_nord: No product link, so not more details available.")
      return product
    try:
      res = Client(http2=True, timeout=20).get(product.link)
    except:
      logger.debug("Aldi_nord: Link broken %s", product.link)
      return product
    if res.status_code != 200:
      logger.debug("Aldi_nord: Unexpected status code from %s", product.link)
      return product
    parsed = BeautifulSoup(res.text, "html.parser")
    desc_wrapper_raw = parsed.select("div.mod.mod-copy")
    if len(desc_wrapper_raw) == 0:
      logger.debug("Aldi_nord: No product details on page / or different pattern. %s", product.link)
      return product
    desc_wrapper_raw = desc_wrapper_raw[0]
    if li_tags := desc_wrapper_raw.find_all("li"):
      description = list()
      for li_tag in li_tags:
        desc = "".join([text for text in li_tag.find_all(string=True) if text != "\n"])
        description.append(desc)
      description = ", ".join(description)
      if product.description != None:
        product.description = product.description + " | " + description
      elif description != "":
        product.description = description
    else:
      return product

    return product

  @staticmethod
  def get_products() -> list[Product]:
    all_products = list()
    url = "https://www.aldi-nord.de/angebote.html"
    try:
      res = Client(http1=True).get(url)
    except:
      logger.warning("Aldi_nord: Problem with request to: %s", url)
    if res.status_code != 200:
      logger.warning("Aldi_nord: unexpected status code %s from request %s", res.status_code, url)
      return None
    parsed = BeautifulSoup(res.text, "html.parser")
    products_raw = parsed.find_all(lambda x: x.name == "div" and x.has_attr("data-tile-url") and re.search(r".articletile.", x.get("data-tile-url")))
    if products_raw == None or len(products_raw) == 0:
      logger.warning("Aldi_nord: No products found / or different pattern. %s", url)
      return None
    base_url = "https://www.aldi-nord.de"
    for product_raw in products_raw:
      partial_url = product_raw.get("data-tile-url")
      if partial_url == None or partial_url == "":
        logger.warning("Aldi_nord: No partial url found.")
        continue
      product_url = base_url + str(partial_url)
      logger.info("Aldi_nord: Parsing %s ...", product_url)
      product = Aldi_nord._get_and_parse_product(product_url)
      if product != None:
        product = Aldi_nord.get_product_details(product)
        all_products.append(product)
    return all_products


class Aldi_sued:
 
  TYPE = "aldi_sued"

  @staticmethod
  @delay_range()
  def _partial_week_get_product_details(product: Product) -> Product:
    """Calls product.link and extracts more description."""
    if product.link == "":
      logger.debug("Aldi_sued: no link for %s", product)
      return product
    try:
      res = Client(http2=True).get(product.link)
    except:
      logger.debug("Aldi_sued: Problem with request to: %s", product.link)
      return product
    if res.status_code != 200:
      print("Problem with aldi sued _get_product_details_..., url: ", product.link)
      return product
    parsed = BeautifulSoup(res.text, "html.parser")
    infoboxes = parsed.find_all("div", class_="infobox")
    if len(infoboxes) == 0:
      logger.debug("Aldi_sued: no info data found in %s", product.link)
      return product
    infos = list()
    for infobox in infoboxes:
      infobox_infos = infobox.find_all(string=True)
      infos.extend(infobox_infos)
    infos = [info.strip(" \n\t\xa0") for info in infos]
    infos = [info for info in infos if info != "" if info != "Zum Garantieportal"]
    infos = ", ".join(infos)
    if infos != "":
      product.description = infos

    return product
  
  @staticmethod
  def _partial_week_parse_product(product_raw: element.Tag, producers: list[str], url_date: date, url: str) -> Product:
    product = Product()
    product.market_type = Aldi_sued.TYPE
    base_url = "https://www.aldi-sued.de"
    # link
    if (a := product_raw.find("a")) != None and (href := a.get("href")) != None:
      partial_link = href
      if partial_link.startswith("https"):
        # exclude discounts you only get in the online-shop
        return None
      link = base_url + partial_link
      product.link = link
    else:
      logger.debug("Aldi_sued: no link for product in %s", url)
    # name
    if (a := product_raw.find("a")) != None and (div := a.find("div")) != None and (h2 := div.find("h2")) != None:
      name = h2.string
      if (n := name.strip()) != "":
        product.name = str(n)
      # producer
      producer = None
      for prod in producers:
        if name.startswith(prod):
          producer = prod
          break
        elif name.startswith(prod.replace(" ", "")):
          producer = prod
          break
      if producer == None:
        logger.debug("Aldi_sued: no producer for product in %s", url)
    else:
      logger.debug("Aldi_sued: no name (-> i.e. also no producer) for product in %s", url)
    # price, price_before, description
    if (a := product_raw.find("a")) != None and (div := a.find("div")) != None and (spans := div.find_all("span")) != None:
      for span_tag in spans:
        # price
        if span_tag.get("class") != None and "at-product-price_lbl" in span_tag.get("class") and "price" in span_tag.get("class"):
          price = span_tag.string.strip()
          price = price.removeprefix("€").strip()
          product.price = str(price)
        # price_before
        if span_tag.get("id") != None and "uvp-plp" in span_tag.get("id"):
          price_before = span_tag.string.strip()
          price_before = str(price_before)
          if price_before != "":
            product.price_before = price_before
        # description
        if span_tag.get("class") != None and "additional-product-info" in span_tag.get("class"):
          desc = " ".join([i.strip() for i in span_tag.string.split("\n") if i.strip() != ""])
          if desc != "":
            product.description = desc
    else:
      logger.debug("Aldi_sued: no price, price_before, description for product in %s", url)
    # extract from description quantity, base_price, base_price_unit (if available)
    if product.description != None and product.description != "":
      description = product.description
      if re.search(r".*\(.*=.*\)", description):
        quantity, base_price_raw = description.removesuffix(")").split("(")
        product.quantity = quantity.strip()
        base_price, base_price_unit = base_price_raw.split("=")
        product.base_price = base_price.strip().removeprefix("1").strip()
        product.base_price_unit = base_price_unit.strip().removeprefix("€").strip()
      else:
        product.quantity = description
    product.description = None  # extracted quantity and base_price data
    # valid_from
    product.valid_from = url_date
    # valid_to is assumed to be the end of the week (saturday)
    product.valid_to = url_date + timedelta(days=(5 - url_date.weekday()))
    # unique_id
    if (uid := re.search(r".+?(\d+).html", product.link)) != None:
      product.unique_id = str(uid.group(1))
      product.unique_id_internal = product.unique_id
    else:
      logger.debug("Aldi_sued: no unique id for product in %s", url)

    return product

  @staticmethod
  @delay_range()
  def _partial_week_get_products_from_url(url: str, url_date: date) -> list[Product]:
    try:
      res = Client(http1=True).get(url)
    except:
      logger.warning("Aldi_sued: Problem with request to: %s", url)
      return None
    if res.status_code != 200:
      logger.warning("Aldi_sued: unexpected status code %s from request to %s", res.status_code, url)
      return None
    parsed = BeautifulSoup(res.text, "html.parser")
    producers = list()
    producers_raw = parsed.find(id="filter-list-brandName")
    if producers_raw != None and len(pr := producers_raw.find_all("label")) != 0:
      producers_raw = pr
      for label_tag in producers_raw:
        producer = "".join(label_tag.stripped_strings)
        if (p := re.search(r".+(?=\(\d+\))", producer)) != None:
          producer = p.group()
          producer = producer.strip()
          producers.append(producer)
        else:
          logger.debug("Aldi_sued: no producer in label-tag in %s", url)
    else:
      logger.debug("Aldi_sued: no brand names in %s", url)
    products = list()
    if (div := parsed.find("div", id="plpProducts")) != None and (articles := div.find_all("article", class_="wrapper")) != None:
      products_raw = articles
    else:
      logger.debug("Aldi_sued: no products in %s", url)
    for product_raw in products_raw:
      product = Aldi_sued._partial_week_parse_product(product_raw, producers, url_date, url)
      if product == None:
        continue
      product = Aldi_sued._partial_week_get_product_details(product)
      products.append(product)

    return products

  @staticmethod
  def _partial_week_get_urls() -> tuple[list[str], list[date]]:
    # get only the urls for the current week 
    now = localtime()
    week_start, week_end = set_week_start_and_end(now)

    base_url = "https://www.aldi-sued.de"
    url = "https://www.aldi-sued.de/de/angebote.html"
    try:
      res = Client(http1=True).get(url)
    except:
      logger.warning("Aldi_sued: Problem with request to: %s", url)
      return None
    if res.status_code != 200:
      logger.warning("Aldi_sued: unexpected status code %s from request to %s", res.status_code, url)
      return None
    parsed = BeautifulSoup(res.text, "html.parser")
    if (div := parsed.find("div", id="subMenu-1")) != None and (partial_urls := div.find_all("a", href=re.compile(r"/de/angebote/d\.\d+-\d+-\d+\.html"))) != None:
      urls = list()
      for u in partial_urls:
        if (href := u.get("href")) != None:
          urls.append(base_url + href)
      if len(urls) == 0:
        logger.debug("Aldi_sued: no urls for partial week found.")
    relevant_urls = list()
    url_dates = list()
    for u in urls:
      date_in_url_raw = re.search(r"\d+-\d+-\d+", u).group()
      day, month, year = date_in_url_raw.split("-")
      url_date = date(int(year.strip()), int(month.strip()), int(day.strip()))
      if url_date >= week_start and url_date <= week_end:
        relevant_urls.append(u)
        url_dates.append(url_date)
    # normally we have Montag-, Donnerstag-, Freitag- and Samstag-URLs, i.e. 4 urls
    # if we don't get whose (e.g. at the end of the week) we create the urls ourselves
    if len(relevant_urls) < 4:
      logger.info("Aldi_sued: not enough relevant urls -> create them")
      relevant_urls = list()
      url_dates = list()
      days = [0, 3, 4, 5]  # monday, thursday, friday, saturday
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
  def _whole_week_bs4_extract_products(tag) -> bool:
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
  def _whole_week_parse_product(product_raw: element.Tag, url: str) -> Product:
    """Parse raw html product to Product."""
    product = Product()
    product.market_type = Aldi_sued.TYPE
    # valid_from and valid_to
    now = localtime()
    dates = None
    for before in product_raw.previous_elements:  # TODO: try product_raw.find_all_previous(..)
      dates = re.findall(r"\d{1,2}\.\d{1,2}(?=\.)", str(before.string))  # TODO: what is standing here between the years?
      if len(dates) == 2:
        break
    if dates != None:
      valid_from_day, valid_from_month = dates[0].split(".")
      valid_from = date(now.tm_year, int(valid_from_month), int(valid_from_day))
      product.valid_from = valid_from
      valid_to_day, valid_to_month = dates[1].split(".")
      valid_to = date(now.tm_year, int(valid_to_month), int(valid_to_day))
      product.valid_to = valid_to
      # from monday to saturday only get the products of the current week.
      # on sunday we can get the products of the next week
      week_start, week_end = set_week_start_and_end(now)
      if valid_from < week_start or valid_from > week_end:
        logger.debug("Aldi_sued: Product not in the current/selected week.")
        return None
    else:
      logger.debug("Aldi_sued: no dates found in product from %s", url)

    # name
    if (fc := product_raw.find("figcaption")) != None and (h3 := fc.find("h3")) != None and (h3c := h3.contents) != None and len(h3c) != 0:
      name = h3c[0]
      product.name = str(name)
    else:
      logger.debug("Aldi_sued: no name found in product from %s", url)
    # the img-tag before figcaption has an attribute called data-asset-id, that is used for the unique_id/unique_id_internal
    if (img_tag := product_raw.find("img")) != None:
      unique_id = img_tag.get("data-asset-id")
      product.unique_id = unique_id
      product.unique_id_internal = product.unique_id
    else:
      logger.debug("Aldi_sued: couldn't find a unique id from img-tag.")
    # price, price_before, producer, description
    if (fc := product_raw.find("figcaption")) != None and (p := fc.find_all("p")) != None:
      p_tags = p
    else:
      logger.debug("Aldi_sued: no p-tags in product from %s", url)
    for index, p_tag in enumerate(p_tags):
      # with 4 p-tags we have no producer field. and the description is in the producer field
      if len(p_tags) == 5:
        match index:
          case 2:
            # price
            if (ptc := p_tag.contents) != None:
              price = ptc[0]
              price = price.strip().removeprefix("€").removesuffix("*").strip()
              product.price = str(price)
            else:
              logger.debug("Aldi_sued: no price in product from %s", url)
            # price_before
            if (pts := p_tag.find("span")) != None and (ptss := pts.find("s")) != None:
              price_before = ptss.string
              product.price_before = str(price_before)
            else:
              logger.debug("Aldi_sued: no price_before in product from %s", url)
          case 3:
            producer = str(p_tag.string)
            if (p := producer.strip()) != "":
              product.producer = p
            else:
              logger.debug("Aldi_sued: no producer in product from %s", url)
          case 4:
            desc = "".join(p_tag.stripped_strings)
            if desc != "":
              product.description = desc
            else:
              logger.debug("Aldi_sued: no description in product from %s", url)
      elif len(p_tags) == 4:  # no producer field.
        match index:
          case 2:  # price and maybe price_before
            # price
            if (ptc := p_tag.contents) != None:
              price = ptc[0]
              price = price.strip().removeprefix("€").removesuffix("*").strip()
              product.price = str(price)
            else:
              logger.debug("Aldi_sued: no price in product from %s", url)
            # price_before
            if (pts := p_tag.find("span")) != None and (ptss := pts.find("s")) != None:
              price_before = ptss.string
              product.price_before = str(price_before)
            else:
              logger.debug("Aldi_sued: no price_before in product from %s", url)
          case 3:
            desc = "".join(p_tag.stripped_strings)
            if desc != "":
              product.description = desc
            else:
              logger.debug("Aldi_sued: no description in product from %s", url)
      else:
        logger.debug("Aldi_sued: unexpected pattern in product from %s", url)
        break
      
    # TODO: parse product.description: quantity, base_price, base_price_unit are all in there
    #       also with vegetables and fruits you get the origin
    # TODO
    # logger.info(product.description)

    # TODO
    # product = is_valid_product(product)
    # if product == None:
    #   logger.warning("Aldi_nord: %s did not meet property requirements.", url)

    return product

  @staticmethod
  @delay_range()
  def _whole_week_get_products_from_url(url: str) -> list[Product]:
    try:
      res = Client(http1=True).get(url)
    except:
      logger.warning("Aldi_sued: Problem with request to. %s", url)
      return None
    if res.status_code != 200:
      logger.warning("Aldi_sued: unexpected status code %s from request to %s", res.status_code, url)
      return None
    parsed = BeautifulSoup(res.text, "html.parser")
    products = list()
    products_raw = parsed.find_all(Aldi_sued._whole_week_bs4_extract_products)
    if len(products_raw) == 0:
      logger.debug("Aldi_sued: No products found for whole week urls.")
    for product_raw in products_raw:
      product = Aldi_sued._whole_week_parse_product(product_raw, url)
      if product != None:
        products.append(product)
      else:
        logger.debug("Aldi_sued: couldn't parse product from %s", product_raw)
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
      if products != None and len(products) != 0:
        all_products.extend(products)
        logger.info("Aldi_sued: parsed %d products from %s", len(products), url)

    urls_partial_week, url_dates = Aldi_sued._partial_week_get_urls()
    for url, url_date in zip(urls_partial_week, url_dates):
      products = Aldi_sued._partial_week_get_products_from_url(url, url_date)
      if products != None and len(products) != 0:
        all_products.extend(products)

    return all_products


class Penny:
  """"""
  """various penny urls
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

  TYPE = "penny"

  @staticmethod
  def create_vouchers(offer_raw: dict):
    pass # TODO what to do with this?

  @staticmethod
  @delay_range()
  def get_products_with_selling_region(selling_region: str, week_start: date) -> tuple[list[Product], list[str]]:
    all_offers = list()
    all_offer_ids = list()
    calendar_week = week_start.isocalendar().week
    base_url_penny = "https://www.penny.de"
    base_url = f"https://www.penny.de/.rest/offers/{week_start.year}-{calendar_week}"
    url = base_url + "?weekRegion=" + selling_region
    try:
      res = Client(http1=True, timeout=20.0).get(url)
    except:
      logger.warning("Penny: Problem with request to: %s", url)
      return None, None
    if res.status_code != 200:
      logger.warning("Penny: unexpected status code %s from request to %s", res.status_code, url)
      return None, None
    # TODO split this error handling a bit more. (so that it's clearer where the error comes from)
    try:
      markets = res.json()
      # groundwork for valid_from and valid_to
      periods = dict()
      if len(markets) != 0 and markets[0] != None and markets[0].get("categoriesMenuPeriod") != None and markets[0].get("categoriesMenuPeriod").values() != None:
        for period in markets[0]["categoriesMenuPeriod"].values():
          # should lead to {"ab-montag": (datetime.date(2024, 7, 1), datetime.date(2024, 7, 6)), "ab-donnerstag": ...}
          if period.get("slug") != None and period.get("startDayIndex") != None and period.get("endDayIndex") != None:
            periods[period["slug"]] = (week_start + timedelta(days=period["startDayIndex"]), week_start + timedelta(days=period["endDayIndex"]))
      # loop through categories
      if len(markets) != 0 and markets[0].get("categories") != None:
        for category in markets[0]["categories"]:
          # ignore payback offers
          if category.get("name") != None:
            if re.search(r".*payback.*", category["name"]):
              continue
          # set valid_from and valid_to
          for date_pattern, dates in periods.items():
            if category.get("id") != None and category["id"].startswith(date_pattern):
              valid_from = dates[0]
              valid_to = dates[1]
              break
          if valid_from == None or valid_to == None:
            logger.debug("Penny: category id starts with unexpected value - couldn't set valid_from and valid_to - in %s", url)
          # loop through offers
          if category.get("offerTiles") != None:
            for product_raw in category["offerTiles"]:
              product = Product()
              product.market_type = Penny.TYPE
              if product_raw.get("title") == None:
                continue  # ignore marketing tiles
              if product_raw.get("onlyOnline"):
                continue  # ignore online offers
              if product_raw.get("price") != None:
                if re.search(r"rabatt", product_raw["price"], re.IGNORECASE):
                  Penny.create_vouchers(product_raw)  # create discount objects ("RABATT"-tiles)
                  continue
              if product_raw.get("title") != None:
                product.name = product_raw["title"].replace("\xad", "").strip().removesuffix("*")
              else:
                logger.debug("Penny: no title for product in category %s in %s", category.get("title"), url)
              if product_raw.get("price") != None:
                product.price = product_raw["price"].removesuffix("*").removeprefix("je").strip()
                if product.price.endswith(".-"):  # e.g. 10.-  =>  10.00
                  product.price = product.price.removesuffix(".-") + ".00"
              else:
                logger.debug("Penny: no price for product in category %s in %s", category.get("title"), url)
              product.valid_from = valid_from
              product.valid_to = valid_to
              if product_raw.get("uuid") != None:
                product.unique_id = product_raw["uuid"]
                product.unique_id_internal = product.unique_id
              else:
                logger.debug("Penny: no unique id for product in category %s in %s", category.get("title"), url)
              if product_raw.get("detailLinkHref") != None:
                product.link = base_url_penny + product_raw["detailLinkHref"]
              else:
                logger.debug("Penny: no link for product in category %s in %s", category.get("title"), url)
              if product_raw.get("originalPrice") != None:
                product.price_before = product_raw["originalPrice"].strip()
                if product.price.endswith(".-"):  # e.g. 10.-  =>  10.00
                  product.price_before = product.price_before.removesuffix(".-") + ".00"
              else:
                logger.debug("Penny: no original price for product in category %s in %s", category.get("title"), url)
              if product_raw.get("showOnlyWithTheAppBadge"):
                product.app_deal = True
              else:
                product.app_deal = False
              if product_raw.get("quantity"):
                product.quantity = product_raw["quantity"]
              else:
                logger.debug("Penny: no quantity for product in category %s in %s", category.get("title"), url)
              if product_raw.get("basePrice") != None:
                # divide offer_raw["basePrice"] into product.base_price and product.base_price_unit
                base_price_raw = product_raw["basePrice"].replace("\n", "")
                if re.search(r"\(.*=.*\)", base_price_raw):
                  base_price_unit, base_price = base_price_raw.strip("()").split("=")
                  product.base_price_unit = base_price_unit.removeprefix("1").strip()
                  product.base_price = base_price.strip()
                elif re.search(r"\(.*je.*\)", base_price_raw):
                  base_price_unit, base_price = base_price_raw.strip("()").split("je")
                  product.base_price_unit = base_price_unit.removeprefix("1").strip()
                  product.base_price = base_price.strip()
                else:
                  logger.debug("Penny: !TODO! don't know this pattern yet: %s", base_price_raw)
              else:
                logger.debug("Penny: no base price data for product in category %s in %s", category.get("title"), url)
              if product_raw.get("subtitle") != None:  # e.g. "je 250 g (1 kg = 1.76)", "je Packung",
                # check for base_price and base_price_unit
                if re.search(r".*\(.*=.*\).*", product_raw["subtitle"]) != None:
                  quantity_raw, base_price_raw = product_raw["subtitle"].removesuffix(")").split("(")
                  product.quantity = quantity_raw.strip()
                  base_price_unit, base_price = base_price_raw.split("=")
                  product.base_price_unit = base_price_unit.removeprefix("1").strip()
                  product.base_price = base_price.strip()
                else:  # only quantity in "subtitle"
                  product.quantity = product_raw["subtitle"]
              all_offers.append(product)
              all_offer_ids.append(product.unique_id)
          else:
            logger.debug("Penny: no offer tiles in %s", url)
    except Exception as err:
      logger.warning("Penny: could not parse json from %s - %s", url, err)

    return all_offers, all_offer_ids
  
  @staticmethod
  @delay_range()
  def get_product_details(product: Product) -> Product:
    """Call the product.link (gets some product details) add description and change the link."""
    logger.info("Penny: Getting product details for: %s", product)
    if product.link == None or product.link == "":
      logger.debug("Penny: No link in product %s", product)
      return product
    url = product.link
    product.link = product.link.strip().removesuffix("~mgnlArea=main~")
    try:
      res = Client(http2=True).get(url)
    except:
      logger.debug("Penny: Problem with request to: %s", url)
      return product
    if res.status_code != 200:
      logger.debug("Penny: product details request to %s failed with %s", url, res.status_code)
      return product
    parsed = BeautifulSoup(res.text, "html.parser")
    details = parsed.find("div", class_="detail-block__body")
    if details != None:
      details = [detail for detail in details.find_all(string=True) if detail != "\n" and detail != "\xa0"]
      if len(details) != 0:
        details = ", ".join(details)
        product.description = details.replace("\xa0", "")
    else:
      logger.debug("Penny: no extra data found in %s", url)

    return product

  @staticmethod
  def get_products(selling_regions: list[str]) -> tuple[list[Product], list[Market_Products]]:
    if selling_regions == None or len(selling_regions) == 0:
      logger.debug("Penny: No selling regions.")
      return None
    logger.info("Penny: got %d selling regions.", len(selling_regions))
    now = localtime()
    week_start, week_end = set_week_start_and_end(now)
    last_update = date(now.tm_year, now.tm_mon, now.tm_mday)
    all_products: set[Product] = set()
    selling_regions_with_product_ids: list[Market_Products] = list()
    for selling_region in selling_regions:
      logger.info("Penny: getting products for selling region %s", selling_region)
      products, product_ids = Penny.get_products_with_selling_region(selling_region, week_start)
      if products != None and product_ids != None:
        logger.info("Penny: %d products for selling region %s.", len(products), selling_region)
        all_products.update(set(products))
        selling_regions_with_product_ids.append(Market_Products(market_type=Penny.TYPE, id=selling_region, product_ids=list(set(product_ids)), week_start=week_start, week_end=week_end, last_update=last_update))
    all_products = list(all_products)
    logger.info("Penny: got %d products over all %d selling regions.", len(all_products), len(selling_regions))
    all_products = list(map(Penny.get_product_details, all_products))
    logger.info("Penny: Discount retrieval returning with %d products.", len(all_products))

    return all_products, selling_regions_with_product_ids


# TODO logging and error catching!
class Rewe:
  """
  Since every rewe market can have different discounts we need a way to cut storage
  a bit. Because with approx. 3700 markets in Germany and approx. 300 discounts
  per market we have 1.110.000 discounts to store per week. 
  Use the unique id of every product (but modified with the digits of the price): 
  in one table we have the products
  and in another we have the markets and the product-ids.
  -> maybe we find a way to only call certain markets (figure out the markets, 
  that always have the same discounts)
  """

  TYPE = "rewe"
  
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
  def get_raw(market_id: str):
    files = os.listdir(Rewe.TMP_PATH)
    if Rewe.PRIVATE_KEY_FILENAME not in files or Rewe.CERTIFICATE_FILENAME not in files:
        get_rewe_creds(source_path=Rewe.TMP_PATH, key_filename=Rewe.PRIVATE_KEY_FILENAME, cert_filename=Rewe.CERTIFICATE_FILENAME)
    
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

    return raw_products


  @staticmethod
  def get_products_with_market_id(market_id: str) -> tuple[list[Product], list[str]]:
    
    raw_products = Rewe.get_raw(market_id)
    if raw_products == None:
      logger.warning("Rewe: problem with raw-data retrieval.")
      return None, None

    all_products = list()
    all_product_ids = list()
    # valid_to # TODO
    valid_to = localtime(raw_products["untilDate"] / 1000)
    valid_to = date(valid_to.tm_year, valid_to.tm_mon, valid_to.tm_mday)
    #if valid_to.weekday() == 6:  # sunday -> saturday for consistency; fyi indicates the market is open on sundays. # undo: respect the data!!
    #  valid_to -= timedelta(days=1)
    valid_from = valid_to - timedelta(days=valid_to.weekday())
    # valid_to is set to saturday and for some to sunday
    for category in raw_products["categories"]:
      if re.search(r".*PAYBACK.*", category["title"]):
        continue
      for product_raw in category["offers"]:
        if product_raw["title"] == "":
          continue
        product = Product()
        product.market_type = Rewe.TYPE
        # name
        product.name = product_raw["title"]
        # valid_to
        product.valid_to = valid_to 
        # valid_from <- assumed to be the start of the week
        product.valid_from = valid_from
        # price
        product.price = product_raw["priceData"]["price"].removesuffix("€").strip()
        # description
        product.description = product_raw["subtitle"]
        # extract quantity, base_price, base_price_unit from offer["subtitle"]
        if base_price_raw := re.search(r".*?(\(.*?=.*?\)).*", product.description):
          base_price_raw = base_price_raw.group(1)
          product.description = product.description.replace(base_price_raw, "")
          base_price_unit, base_price = base_price_raw.split("=")
          product.base_price = base_price.removesuffix(")").strip()
          product.base_price_unit = base_price_unit.removeprefix("(1").strip()
        # detail.contents has a Art.-Nr. and can have a Hersteller and sometimes also a Herkunft
        for line in product_raw["detail"]["contents"]:
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
        if (detail := product_raw.get("detail")) and (pitch_in := detail.get("pitchIn")):
            if product.description:
              product.description += ", " + pitch_in
            else:
              product.description = pitch_in


        # price_before, link <- missing
        product.unique_id_internal = product.unique_id + cust_hash([product.price, 
                                                                    product.description,
                                                                    product.valid_to])

        all_products.append(product)
        all_product_ids.append(product.unique_id_internal)

    return all_products, all_product_ids


  @staticmethod
  @delay_range()
  def get_product_details(product: Product) -> Product:
    # TODO: call different rewe api
    # write into different database _> general db for products with a special 
    #   column for rewe_article_id and listing_id (for adding to basket)
    # contains ingredients, contact_info, gtin/ean, current_retail_price, nutrition_facts, ...


    return product
    

  @staticmethod
  def get_products(market_ids: list[str] | str | int, *, extras: bool = False) -> tuple[list[Product], list[Market_Products]]:
    """"""
    if market_ids == None or len(market_ids) == 0:
      return None, None
    elif isinstance(market_ids, str) or isinstance(market_ids, int):
      market_ids = [str(market_ids)]
    now = localtime()
    last_update = date(now.tm_year, now.tm_mon, now.tm_mday)
    week_start, week_end = set_week_start_and_end(now)
    all_products: set[Product] = set()
    market_ids_with_product_ids: list[Market_Products] = list()
    for market_id in market_ids:
      products, product_ids = Rewe.get_products_with_market_id(market_id)
      all_products.update(set(products))
      market_ids_with_product_ids.append(Market_Products(market_type=Rewe.TYPE, id=market_id, product_ids=list(set(product_ids)), week_start=week_start, week_end=week_end, last_update=last_update))
    all_products = list(all_products)
    if extras:
      all_products = list(map(Rewe.get_product_details, all_products))

    return all_products, market_ids_with_product_ids


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

  @staticmethod
  @delay_range()
  def get_products_with_market_id(market_id: str, for_date: str) -> tuple[set[Product], set[str]]:
    url = f"https://www.hit.de/api/offers?&for_store={market_id}&for_date={for_date}&limit=1000"
    headers = {
      "user-agent": "okhttp/4.12.0",
      "x-api-key": "live:android:cLpsXTCo5pR7URUu8gvSN9yJCPj5A"
    }
    try:
      res = Client(http1=True, headers=headers, timeout=30.0).get(url)
    except:
      logger.warning("HIT: Problem with request to: %s", url)
      return None, None
    if res.status_code != 200:
      logger.warning("HIT: unexpected status code %s from request to %s", res.status_code, url)
      return None, None
    try:
      res = res.json()
    except:
      logger.warning("Hit: could not parse json from %s", url)
      return None, None
    products: set[Product] = set()
    product_ids: set[str] = set()
    data = valid_from = valid_to = None
    if not (data := res.get("data")):
      logger.warning("Hit: data missing for market id %s, data=%s", market_id, data)
      return None, None
    for product_raw in data:
      product = Product()
      product.market_type = Hit.TYPE
      if (name := product_raw.get("headline")):
        product.name = name.replace("\n", " ")
      if (price := product_raw.get("price")):
        product.price = price
      # Often there are two price_before values - 'Preis Vorwoche' (price the week before, key="stringBelowPrice") and a crossed out price.
      # At the moment I take the crossed out price.
      if (price_before := product_raw.get("stringBeforePrice")):
        product.price_before = price_before.replace("*", "")
      if (valid_from := product_raw.get("validFrom")):
        year, month, day = datetime.fromisoformat(valid_from).astimezone(timezone.utc).strftime("%Y-%m-%d").split("-")
        valid_from = date(int(year), int(month), int(day)) + timedelta(days=1)  # hit sets validFrom to sunday..
        product.valid_from = valid_from
      if (valid_to := product_raw.get("validTo")):
        year, month, day = datetime.fromisoformat(valid_to).astimezone(timezone.utc).strftime("%Y-%m-%d").split("-")
        valid_to = date(int(year), int(month), int(day))
        product.valid_to = valid_to
      if (url := product_raw.get("url")):
        product.link = url
      if (id := product_raw.get("id")):
        product.unique_id = str(id)
        product.unique_id_internal = product.unique_id
        product_ids.add(str(id))

      if (description := product_raw.get("text")):
        reduced_description = list()
        for desc_part in description.replace("\n\n", "\n").split("\n"):
          # extract base_price, base_price_unit
          if (base_price_raw := re.search(r".*?(\(.*?=.*?\)).*", desc_part)):
            base_price_raw = desc_part  #base_price_raw.group(1)
            product.description = description.replace(base_price_raw, "")
            base_price_unit, base_price = base_price_raw.split("=")
            product.base_price = base_price.removesuffix(")").strip()
            product.base_price_unit = base_price_unit.removeprefix("(").removeprefix("1").strip()
            desc_part = ""
          # extract quantity; sometimes you get more than one match, like ['ca. 300 g', 'verschiedene Würzungen, 100 g'],
          #                   the assumption is that the second one is correct and wanted.
          if re.search(r"(.*?\d+\s?(kg|l|ml|g)\b|Stück|.*?\d+.*?Packung|.*?\d+\s?Anwendung(en)?)", desc_part, re.IGNORECASE):
            product.quantity = desc_part
          reduced_description.append(desc_part)
        product.description = ", ".join([desc for desc in [desc for desc in reduced_description if desc != product.quantity] if desc != ""])
        if product.description == "":
          product.description = None

      # some extra infos for description, e.g. 'Vegan'
      if (labels := product_raw.get("labels")):
        extra_infos = list()
        for label in labels:
          if (extra_info := label.get("label")):
              extra_infos.append(extra_info)
        if product.description == None:
          product.description = ", ".join(extra_infos)
        else:
          product.description = product.description + ", " + ", ".join(extra_infos)
      
      products.add(product)
    
    return products, product_ids


  @staticmethod
  def get_products(market_ids: list[str]) -> tuple[list[Product], list[Market_Products]]:
    if market_ids == None or len(market_ids) == 0:
      logger.debug("Hit: No market ids.")
      return None, None
    all_products: set[Product] = set()
    market_ids_with_product_ids: list[Market_Products] = list()
    # set request date
    now = localtime()
    week_start, week_end = set_week_start_and_end(now)
    last_update = date(now.tm_year, now.tm_mon, now.tm_mday)
    for_date = week_start.strftime("%Y-%m-%d")
    for market_id in market_ids:
      products, product_ids = Hit.get_products_with_market_id(market_id, for_date)
      if products != None and product_ids != None:
        all_products.update(products)
        market_ids_with_product_ids.append(Market_Products(market_type=Hit.TYPE, id=market_id, product_ids=list(product_ids), week_start=week_start, week_end=week_end, last_update=last_update))
    all_products = list(all_products)

    return all_products, market_ids_with_product_ids
      

class Netto:
  
  TYPE = "netto"
  PRODUCER_MAPPING = None

  @staticmethod
  @delay_range()
  def get_producer_mapping() -> dict[str, str]:
    hard_coded_brands = {
      "backstube": "Backstube (Marktbäckerei)",
      "gut ponholz": "Gut Ponholz",
      "hofmaier": "Hofmaier",
      "wolf echt gute wurst": "Wolf - echt gute Wurst",
    }
    url = "https://www.netto-online.de/marken"
    headers = {"user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
               "host": "www.netto-online.de"}
    res = Client(http2=True, headers=headers).get(url)
    if res.status_code != 200:
      return None
    parsed = BeautifulSoup(res.text, "html.parser")
    brands_raw = parsed.find_all("a", class_="brand-link")
    brands = hard_coded_brands
    for brand_raw in brands_raw:
      key = brand_raw.get("href")
      key = key.split("/")[-1]
      key = key.replace("-", " ").replace("_", " ").lower().removesuffix("-")
      value = brand_raw.get_text()
      brands.update({key: value})
    
    return brands


  @staticmethod
  @delay_range()
  def get_raw(market_id: str):
    url = "https://www.clickforbrand.de/offers-qa/rest/v1/offers?store_id=" + market_id
    headers = {
      "user-agent": "NettoApp/7.1.2 (Build: 7.1.2.1; Android 11)",
      "x-netto-api-key": "78b72bf5-3229-4972-88df-486bcf6191bb",
    }
    try:
      res = Client(http1=True, headers=headers).get(url)
    except:
      # TODO: LOGGER! wie in marketlists
      return None
    if res.status_code != 200:
      # Logger
      return None
    try:
      discounts_raw = res.json()
    except:
      # Logger
      return None
    if ((data := discounts_raw.get("data")) != None) and (len(data) != 0):
      return data
    else:
      # logger
      return None

  @staticmethod
  def get_products_with_market_id(market_id: str) -> tuple[set[Product], set[str]]:
    if Netto.PRODUCER_MAPPING == None:
      Netto.PRODUCER_MAPPING = Netto.get_producer_mapping()
    products_raw = Netto.get_raw(market_id)
    all_products = set()
    all_product_ids = set()
    for category in products_raw:
      valid_from = valid_to = articles = None
      if not ((valid_from := category.get("offer_date_valid_from")) 
              and (valid_to := category.get("offer_date_valid_to"))
              and (articles := category.get("article"))):
        # logger
        continue
      # convert valid_to and valid_from
      date_format = "%Y-%m-%d %H:%M:%S"
      valid_from = datetime.strptime(valid_from, date_format)
      valid_from = date(valid_from.year, valid_from.month, valid_from.day)
      valid_to = datetime.strptime(valid_to, date_format)
      valid_to = date(valid_to.year, valid_to.month, valid_to.day)
      for article in articles:
        is_online = article.get("isOnline")
        if is_online == "true":
          continue
        if not ((name := article.get("title"))
                and (price_raw := article.get("price"))
                and (price := price_raw.get("price"))
                and (id := article.get("artikelID"))):
          # logger
          continue
        product = Product()
        product.market_type = Netto.TYPE
        product.valid_from = valid_from
        product.valid_to = valid_to
        product.name = name
        product.unique_id = id
        price = price.removesuffix("*")
        if price.endswith(".-"):
          product.price = price.removesuffix(".-") + ".00"
        else:
          product.price = price
        if (quantity := article.get("text_gebinde")) == "":
          product.quantity = "Stück"  # TODO: check this!
        else:
          product.quantity = quantity
        if (base_price_raw := article.get("hp_grundpreis")) != "":
          base_price_raw = base_price_raw.split("/")
          if len(base_price_raw) >= 2:
            base_price = base_price_raw[:-1]
            base_price = "/".join(base_price)
            base_price_unit = base_price_raw[-1]
          else:
            pass # logger
          base_price = base_price.strip()
          if base_price.endswith(".-"):
            product.base_price = base_price.removesuffix(".-") + ".00"
          else:
            product.base_price = base_price
          product.base_price_unit = base_price_unit.strip()
        if (price_before := price_raw.get("save_price")) != "":
          if (price_before_match := re.search(r"\d+", price_before)) != None and (price_before_match.group() != ""):
            price_before = price_before.strip().removeprefix("UVP").removeprefix("statt").strip()
            if price_before.endswith(".-"):
              product.price_before = price_before.removesuffix(".-") + ".00"
            else:
              product.price_before = price_before
            
        # extract information in disturber-values
        for key, value in article.items():
          if key.startswith("disturber") and value != None:
            # origin
            if value.get("type") == "pin_regionales_aus":
              origin_city = value.get("text")
              if product.origin:
                product.origin = ", ".join([origin_city, product.origin])
              else:
                product.origin = ", ".join([origin_city, "Deutschland"])
              continue

            if (img := value.get("img")):
              img_filename = img.split("/")[-1]  # extract filename
              img_filename = ".".join(img_filename.split(".")[:-1])  # remove file-format
              img_filename = img_filename.replace("-", " ").replace("_", " ").lower().removesuffix("-")
              
              # producer
              if Netto.PRODUCER_MAPPING != None and product.producer == None:
                img_filename_parts = img_filename.split()
                for start in range(0, len(img_filename_parts)):
                  for end in range(len(img_filename_parts), start, -1):
                    partial_img_filename = " ".join(img_filename_parts[start:end])
                    producer = Netto.PRODUCER_MAPPING.get(partial_img_filename)
                    if producer != None:
                      product.producer = producer
                      print(product)
                      break
                  if product.producer != None:
                    break
              # origin
              if product.origin:
                continue
              else:
                origin_raw = re.search(r"(deutschlandfahne|ehfe logo)", img_filename)
                if origin_raw:
                  product.origin = "Deutschland"

        description = article.get("description_short").replace("<br />", ", ")
        if (description_extra := article.get("text_pfand")):                        # (Flaschen-)Pfand-Information
          description = ", ".join([description, description_extra])
        if (description_extra := article.get("text_more_info")):
          description = ", ".join([description, description_extra])
        product.description = description
        product.unique_id_internal = product.unique_id + cust_hash([product.price])

        # Check for app_deal
        begin_app_deal_text = re.search(r"<strong>", description)
        netto_app_preis_marker = re.search(r"Netto-App-Preis", description)
        end_app_deal_text = re.search(r"</strong>", description)
        if begin_app_deal_text and end_app_deal_text and netto_app_preis_marker:
          begin_disclaimer_text = re.search(r"<em>", description)
          end_disclaimer_text = re.search(r"</em>", description)
          if begin_disclaimer_text and end_disclaimer_text:
            begin_disclaimer_text = begin_disclaimer_text.start()
            end_disclaimer_text = end_disclaimer_text.end()
            if end_disclaimer_text == len(description):
              description = description[:begin_disclaimer_text]
            else:
              description = description[:begin_disclaimer_text] + ", " + description[end_disclaimer_text:]
            description = description.strip(", ")
          begin_app_deal_text = begin_app_deal_text.start()
          end_app_deal_text = end_app_deal_text.end()
          app_deal_text = description[begin_app_deal_text:end_app_deal_text]
          if end_app_deal_text == len(description):
            description = description[:begin_app_deal_text]
          else:
            description = description[:begin_app_deal_text] + ", " + description[end_app_deal_text:]
          description = description.strip(", ")
          app_deal_text = BeautifulSoup(app_deal_text, "html.parser")
          app_deal_text = app_deal_text.get_text()
          app_deal_base_price_raw = re.search(r"\((.*/.*)\)", app_deal_text)
          app_deal_base_price = None
          if app_deal_base_price_raw:
            app_deal_base_price, _ = app_deal_base_price_raw.group(1).split("/")
            app_deal_base_price = app_deal_base_price.strip()
            app_deal_text = app_deal_text.replace(app_deal_base_price, "")
          app_deal_price_raw = re.search(r"\d\.(-|\d*)", app_deal_text)
          if app_deal_price_raw:
            app_deal_price = app_deal_price_raw.group().removesuffix("*")
          product.description = description
          product_app_deal = copy(product)
          product_app_deal.base_price = app_deal_base_price
          if app_deal_price.endswith(".-"):
            product_app_deal.price = app_deal_price.removesuffix(".-") + ".00"
          else:
            product_app_deal.price = app_deal_price
          product_app_deal.unique_id_internal = product_app_deal.unique_id + cust_hash([product_app_deal.price])
          product_app_deal.app_deal = True
          product.app_deal = False
          all_products.add(product_app_deal)
          all_product_ids.add(product_app_deal.unique_id_internal)
        
        all_products.add(product)
        all_product_ids.add(product.unique_id_internal)
    
    if len(all_products) == 0:
      return None, None
    else:
      return all_products, all_product_ids


  @staticmethod
  def get_products(market_ids: list[str] | str | int) -> tuple[list[Product], list[Market_Products]]:
    if market_ids == None or len(market_ids) == 0:
      return None, None
    elif isinstance(market_ids, str) or isinstance(market_ids, int):
      market_ids = [str(market_ids)]
    now = localtime()
    last_update = date(now.tm_year, now.tm_mon, now.tm_mday)
    week_start, week_end = set_week_start_and_end(now)
    all_products: set[Product] = set()
    market_ids_with_product_ids: list[Market_Products] = list()
    for market_id in market_ids:
      products, product_ids = Netto.get_products_with_market_id(market_id)
      if products != None and product_ids != None:
        all_products.update(products)
        market_ids_with_product_ids.append(Market_Products(market_type=Netto.TYPE,
                                                           id=market_id,
                                                           product_ids=list(product_ids),
                                                           week_start=week_start,
                                                           week_end=week_end,
                                                           last_update=last_update))
    all_products = list(all_products)


    return all_products, market_ids_with_product_ids
    
    
class Norma:
  TYPE = "norma"
  
  @staticmethod
  def _parse_price(price_raw: str) -> str:
    price_raw = price_raw.replace(",", ".")
    if price_raw.startswith("-."):
      price = "0" + price_raw.removeprefix("-")
    elif price_raw.endswith(".-"):
      price = price_raw.removesuffix("-") + "00"
    else:
      price = price_raw
    return price
  
  @staticmethod
  def _parse_date(text: str, current_year = None) -> date:
    if text == None:
      return None
    if (match := re.search(r"\d{2}\.\d{2}\.\d{2,4}", text)):
      day, month, year = match.group().split(".")
      day = int(day)
      month = int(month)
      year = int(year)
      today = date(year, month, day)
    elif (match := re.search(r"(\d{2}\.\d{2})\.", text)):
      day, month = match.group(1).split(".")
      day = int(day)
      month = int(month)
      today = date(current_year, month, day)
    else:
      today = None
    return today
  
  @staticmethod
  def _is_current_week(text: str, now = None) -> bool:
    """Parse txtSubline, txtTermin on the start-page and check."""
    if now == None:
      now = localtime()
    week_start, week_end = set_week_start_and_end(now)
    today = Norma._parse_date(text, now.tm_year)
    if today == None:
      return True
    if week_start <= today and week_end >= today:
      return True
    else:
      return False
  
  @staticmethod
  @delay_range(2000,6000)
  def get_raw(url: str):
    headers = {
      "authorization": "Basic d3NhcGk6YWhmaWV4b2gzRWVjaGllN3NpR2g="
    }
    try:
      res = Client(http1=True, headers=headers).get(url)
    except:
      # TODO: LOGGER! wie in marketlists
      return None
    if res.status_code != 200:
      # Logger
      return None
    try:
      raw_data = res.json()
    except:
      # Logger
      return None
    if ((data := raw_data.get("data")) != None) and (len(data) != 0):
      if ((content := data.get("content")) != None) and (len(content) != 0):
        return content
      else:
        # logger
        return None
    else:
      # logger
      return None
  
  @staticmethod
  def get_products_with_regio_key(regio_key: str, now) -> tuple[set[Product], set[str]]:
    week_start, week_end = set_week_start_and_end(now)
    products: set[Product] = set()
    product_ids: set[str] = set()
    
    url_main = f"https://www.norma-online.de/ws/?controller=Norma_View&action=remoteView&sRegioKey={regio_key}&sAction=home"
    raw_data = Norma.get_raw(url_main)
    if raw_data == None:
      # logger
      return None
    urls = list()
    for content in raw_data:
      if content.get("type") in ["topic_grid", "slider", "generic_slider"]:
        if content.get("content"):
          for content_inner in content.get("content"):
            if (url := content_inner.get("url")) and url.startswith("/") and Norma._is_current_week(content_inner.get("txtSubline"), now):
              urls.append(url)
        elif content.get("items"):
          for items_inner in content.get("items"):
            if (url := items_inner.get("url")) and url.startswith("/") and not re.search(r"pdf", url):
              urls.append(url)
    for url in urls:
      if url.startswith("/catalog") and (subsite_id := re.search(r"\d+", url)):
        url_subsite = f"https://www.norma-online.de/ws/?controller=Norma_View&action=remoteView&sRegioKey={regio_key}&sAction=catalog&isIdentifier={subsite_id.group()}"
      elif url.startswith("/remote"):
        action_id = url.removeprefix("/remote/")
        url_subsite = f"https://www.norma-online.de/ws/?controller=Norma_View&action=remoteView&sRegioKey={regio_key}&sAction={action_id}"
      else:
        # logger (log url)
        continue
      print(url_subsite)
      raw_data = Norma.get_raw(url_subsite)
      if raw_data == None:
        # logger
        continue
      for content in raw_data:
        if content.get("type") == "product_grid":
          if (content_inner := content.get("content")):
            for raw_product in content_inner:
              #parse product
              if (valid_from_raw := raw_product.get("txtTermin")) and not Norma._is_current_week(valid_from_raw, now):
                # logger
                continue
              if (is_store_offer := raw_product.get("bStore")) and is_store_offer == "false":
                # logger
                continue
              if not ((id := raw_product.get("id"))
                      and (price := raw_product.get("txtVerkaufspreis"))
                      and (title := raw_product.get("txtArtikel"))):
                # logger
                continue
              product = Product()
              product.market_type = Norma.TYPE
              product.name = title
              product.price = Norma._parse_price(price)
              product.unique_id = id
              product.unique_id_internal = id + cust_hash([product.price])
              if (quantity := raw_product.get("txtBezogenAuf")) and quantity != "":
                product.quantity = quantity
              elif (quantity := raw_product.get("txtInhaltLang")) and quantity != "":
                product.quantity = quantity
              if (base_price_raw := raw_product.get("txtGrundpreis")) and base_price_raw != "":
                base_price_raw = base_price_raw.removeprefix("(").removesuffix(")")
                base_price_unit, base_price = base_price_raw.split("=")
                #parse base price unit
                base_price_unit_number, base_price_unit_unit = base_price_unit.strip().split()
                if base_price_unit_number == "1":
                  base_price_unit = base_price_unit_unit.strip()
                else:
                  base_price_unit = base_price_unit.strip()
                product.base_price_unit = base_price_unit
                product.base_price = Norma._parse_price(base_price.strip())
              #parse valid_from
              if valid_from_raw and (valid_from := Norma._parse_date(valid_from_raw, now.tm_year)):
                product.valid_from = valid_from
              else:
                # logger
                product.valid_from = week_start
              product.valid_to = week_end
              #producer
              if (producer := raw_product.get("txtMarke")) and producer != "":
                product.producer = producer
              if (price_before_raw := raw_product.get("txtInfo")) and price_before_raw != "":
                price_before_raw = price_before_raw.lower().removeprefix("uvp").strip()
                price_before = Norma._parse_price(price_before_raw)
                product.price_before = price_before
              # call productDetails-url to get the description
              url_product = f"https://www.norma-online.de/ws/?controller=Norma_View&action=remoteView&sRegioKey={regio_key}&sAction=productDetails&isIdentifier={product.unique_id}"
              product_details_raw = Norma.get_raw(url_product)
              if product_details_raw != None:
                for product_details_raw_content in product_details_raw:
                  if product_details_raw_content.get("type") == "html_text":
                    html = product_details_raw_content.get("html")
                    if html == "":
                      break
                    parsed = BeautifulSoup(html, "html.parser")
                    description = parsed.get_text().replace("\xa0", "").replace("\n", ", ").strip(", ")
                    product.description = description
                    break
              products.add(product)
              product_ids.add(product.unique_id_internal)
    
    return products, product_ids
    
    
  @staticmethod
  def get_products(regio_keys: list[str]) -> tuple[list[Product], list[Market_Products]]:
    all_products: set[Product] = set()
    market_ids_with_product_ids: list[Market_Products] = list()
    now = localtime()
    last_update = date(now.tm_year, now.tm_mon, now.tm_mday)
    week_start, week_end = set_week_start_and_end(now)
    for regio_key in regio_keys:
      products, product_ids = Norma.get_products_with_regio_key(regio_key, now)
      if products != None and product_ids != None:
        all_products.update(products)
        market_ids_with_product_ids.append(Market_Products(market_type=Norma.TYPE,
                                                           id=regio_key,
                                                           product_ids=list(product_ids),
                                                           week_start=week_start,
                                                           week_end=week_end,
                                                           last_update=last_update))
    all_products = list(all_products)
    
    return all_products, market_ids_with_product_ids
    
    



if __name__ == "__main__":
  pass