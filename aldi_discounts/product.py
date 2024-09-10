
import os
import sqlite3
from hashlib import md5
from datetime import date
from dataclasses import dataclass, asdict

product_tablename = "products"  # products

@dataclass(init=False)
class Product:
  """Use hash and eq ONLY when you know that unique id is set and is really unique.
  
  E.g. Penny and Rewe can (and need to) use it.
  ** rewe uses the same 'unique_id' for products with different prices 
     (some regions/market_ids have a cheaper price)
     - so unique_id_internal for rewe is build the following way:
       unique_id + cust_hash(...) (without a seperator), e.g. '1359460' + '...' => '1359460...'
     - for every other market type (that does not need this) it's just the same as unique_id
  """
  market_type: str = None
  name: str = None
  price: str = None
  quantity: str = None
  base_price: str = None   # e.g. 9.95
  base_price_unit: str = None   # e.g. kg
  price_before: str = None
  producer: str = None
  description: str = None
  currency: str = "€"
  valid_from: date = None
  valid_to: date = None
  link: str = None
  origin: str = None
  unique_id: str = None
  unique_id_internal: str = None  # ** (-> docstring)
  app_deal: bool = None
  # pfand/ bottle_deposit
  # nutri_score?
  # lowest_price_last_30_days?

  def dict(self):
    return asdict(self)
  
  @staticmethod
  def primary_key():
    return "name, price, market_type, valid_from, valid_to, unique_id_internal"

  def __eq__(self, other):
    if not isinstance(other, Product):
      return NotImplemented
    return self.unique_id_internal == other.unique_id_internal

  def __hash__(self):
    return hash(self.unique_id_internal)

def create_table(cursor):
  columns = list(Product().dict().keys())
  columns.append(f"PRIMARY KEY ({Product.primary_key()})")
  cursor.execute(f"CREATE TABLE if not exists {product_tablename}({','.join(columns)}) WITHOUT ROWID")
  return cursor

def store(products: list[Product], path: str):
  """sqlite3. simple insert. no checking."""
  if len(products) == 0:
    return None
  con = sqlite3.connect(path)
  cur = con.cursor()
  cur = create_table(cur)
  columns = list(Product().dict().keys())
  cur.executemany(f"REPLACE INTO {product_tablename} VALUES({','.join(['?']*len(columns))})",
                  [tuple(product.dict().values()) for product in products])
  con.commit()
  cur.close()
  con.close()

def products_present(market_type: str, week_start: date, path: str, *, light_check: bool = True) -> bool:
  if not os.path.exists(path):
    return None
  if light_check:
    con = sqlite3.connect(path)
    cur = con.cursor()
    cur.execute(f"SELECT * FROM {product_tablename} WHERE market_type='{market_type}' AND valid_from>='{week_start}'")
    one = cur.fetchone()
    if one != None and len(one) != 0:
      return True
    else:
      return False
  else:
    # TODO Check how many products are present
    return None
  
def cust_hash(values: list[str|int|float]) -> str:
  """md5"""
  values = "".join([str(value) for value in values if value != None])
  return md5(values.encode(), usedforsecurity=False).hexdigest()
  

def is_valid_product(product: Product) -> Product | None:
  """Check for certain absolutely necessary properties."""
  #TODO: which properties?
  # name, price, quantity, type, valid_from, valid_to, unique_id(_internal) <- should also be enough for a primary key
  pass

if __name__ == "__main__":
  res = products_present("hit", date(2024, 7, 21), "./data/products.db")
  print(res)