
import os
import sqlite3
from dataclasses import dataclass, asdict
from datetime import date

market_products_tablename = "market_products"

@dataclass(init=True)
class Market_Products:
  """One market or selling region (identified by an id) with all offer ids available there.
  
  Used to store products/offers only once.
  """
  id: str = None
  market_type: str = None
  product_ids: list | str = None
  week_start: date = None
  week_end: date = None
  last_update: date = None
  
  def __eq__(self, other):
    if not isinstance(other, Market_Products):
      return NotImplemented
    return self.id == other.id
  
  def __hash__(self):
    return hash(self.id)

  def dict(self):
    return asdict(self)
  
  @staticmethod
  def primary_key():
    return "id, market_type"
  

def create_table(cursor):
  columns = list(Market_Products().dict().keys())
  columns.append(f"PRIMARY KEY ({Market_Products.primary_key()})")
  cursor.execute(f"CREATE TABLE if not exists {market_products_tablename}({','.join(columns)}) WITHOUT ROWID")
  return cursor

def store(market_products: Market_Products, path: str):
  """Primarily for hit and rewe."""
  if market_products == None:
    return None
  con = sqlite3.connect(path)
  cur = con.cursor()
  cur = create_table(cur)
  columns = list(Market_Products().dict().keys())
  market_products.product_ids = ",".join(market_products.product_ids) if market_products.product_ids != None else None
  cur.execute(f"REPLACE INTO {market_products_tablename} VALUES({','.join(['?']*len(columns))})",
              tuple(market_products.dict().values()))
  con.commit()
  cur.close()
  con.close()

def store_multiple(multiple_market_products: list[Market_Products], path: str, delete: bool = False):
  """delete: True = Delete all rows before inserting new data.
  """
  if len(multiple_market_products) == 0:
    return None
  con = sqlite3.connect(path)
  cur = con.cursor()
  cur = create_table(cur)
  if delete:
    cur.execute(f"DELETE FROM {market_products_tablename}")
    con.commit()
  columns = list(Market_Products().dict().keys())
  market_products_parsed: list[Market_Products] = list()
  for market_products in multiple_market_products:
    market_products.product_ids = ",".join(market_products.product_ids) if market_products.product_ids != None else None
    market_products_parsed.append(market_products)
  cur.executemany(f"REPLACE INTO {market_products_tablename} VALUES({','.join(['?']*len(columns))})",
                  [tuple(market_products.dict().values()) for market_products in market_products_parsed])
  con.commit()
  cur.close()
  con.close()

def get_all_ids(path: str) -> list[str]:
  """Primarily for retrieving the selling region discounts."""
  if not os.path.exists(path):
    return None
  con = sqlite3.connect(path)
  cur = con.cursor()
  cur.execute(f"SELECT id FROM {market_products_tablename}")
  ids = cur.fetchall()
  ids = [id[0] for id in ids]
  if len(ids) == 0:
    return None
  cur.close()
  con.close()
  return ids