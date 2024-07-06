
from time import localtime

from httpx import Client

from market import Market

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



if __name__ == "__main__":
  Penny.create_marketlist_penny()