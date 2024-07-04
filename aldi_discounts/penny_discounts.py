from time import localtime
from datetime import date

from httpx import Client

class Penny:

  def get_products():
    now = localtime()
    day, month, year = now.tm_mday, now.tm_mon, now.tm_year
    weekday = date(year, month, day).isocalendar().week
    url = f"https://www.penny.de/.rest/offers/{year}-{weekday}"
    res = Client().get(url)
    if res.status_code != 200:
      print("something wrong with penny market list request")
      return None
    markets = res.json()
    offers = markets[0]["categories"][0]["offerTiles"]
    print(offers[7].keys())


if __name__ == "__main__":
  Penny.get_products()


  """
  
  https://www.penny.de/.rest/market/markets/market_230533

  https://www.penny.de/.rest/marketRegion/63302533

  https://www.penny.de/.rest/offers/2024-27?weekRegion=15A-05
  
  https://www.penny.de/markt/darmstadt/230533/penny-pallas-pallaswiesenstr-70-72

  take a llok at the app
  """