import re
import time
from datetime import timedelta, date

import requests
from bs4 import BeautifulSoup, element


class Product:

  def __init__(self):
    self._name = ""
    self._price = ""
    self._price_before = ""
    self._producer = ""
    self._description = ""
    self._currency = "€"
    self._valid_from = ""
    self._valid_to = ""
    self._link = ""

  @property
  def name(self):
    return self._name
  
  @name.setter
  def name(self, name):
    self._name = name

  @property
  def price(self):
    return self._price
  
  @price.setter
  def price(self, price):
    self._price = str(price).replace('.', ',')

  @property
  def price_before(self):
    return self._price
  
  @price_before.setter
  def price_before(self, price):
    self._price_before = str(price).replace('.', ',')

  @property
  def producer(self):
    return self._producer
  
  @producer.setter
  def producer(self, producer):
    self._producer = producer
  
  @property
  def description(self):
    return self._description
  
  @description.setter
  def description(self, description):
    self._description = description
  
  @property
  def valid_from(self):
    return self._valid_from
  
  @valid_from.setter
  def valid_from(self, valid_from):
    self._valid_from = valid_from

  @property
  def valid_to(self):
    return self._valid_to
  
  @valid_to.setter
  def valid_to(self, valid_to):
    self._valid_to = valid_to

  @property
  def link(self):
    return self._link
  
  @link.setter
  def link(self, link):
    self._link = link  


class Aldi_sued:
 
  @staticmethod
  def _get_urls_partial_week() -> list[str]:
    # TODO find a way to get only the urls for the current week  OR  if it's sunday the urls for the next week.
    now = time.localtime()
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
    res = requests.get(url)
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
  def _parse_product_partial_week(product_raw: element.Tag, producers: list[str]):
    new_product = Product()
    base_url = "https://www.aldi-sued.de"
    partial_link = product_raw.find("a").get("href")
    if partial_link.startswith("https"):
      # discounts you only get in the online-shop
      return None
    else:
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
    now = time.localtime()
    weekday = now.tm_wday
    valid_to = date(now.tm_year, now.tm_mon, now.tm_mday) + timedelta(days=(5 - weekday))
    valid_to = str(valid_to.day) + "." + str(valid_to.month) + "." + str(valid_to.year)
    new_product.valid_to = valid_to
    # print(valid_from, valid_to, name, price, price_before, producer, desc, link, sep=" | ")
    return new_product

  @staticmethod
  def _parse_product_whole_week(product_raw: element.Tag):
    new_product = Product()
    # valid from/to
    current_year = time.localtime().tm_year
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
  def _get_products_from_url_whole_week(url: str):
    res = requests.get(url)
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
  def _get_products_from_url_partial_week(url: str):
    res = requests.get(url)
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
  def get_products():
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


if __name__ == "__main__":
  res = Aldi_sued.get_products()
  print(len(res))
  print(res[33].name)