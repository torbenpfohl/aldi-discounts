import re
import sqlite3
from time import localtime
from datetime import date, timedelta

from httpx import Client
from bs4 import BeautifulSoup

from util import delay
from product import Product

class Penny:

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



if __name__ == "__main__":
  ps, ps_ids = Penny.get_products_with_selling_region("15A-02")
  print(len(ps_ids))
  for p in ps:
    p = Penny.get_product_details(p)
    print(p)









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