from httpx import Client

def store_markets():
  pass

def create_marketlist_penny():
  url = "https://www.penny.de/.rest/market"
  res = Client().get(url)
  if res.status_code != 200:
    print("something wrong with penny market list request")
    return None
  markets = res.json()
  print(markets[0])


if __name__ == "__main__":
  create_marketlist_penny()