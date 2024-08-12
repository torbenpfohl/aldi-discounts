import os
import sqlite3
from datetime import date, datetime
from dataclasses import dataclass, asdict

#__all__ = ["Market", "store", "get_last_update"]

@dataclass(init=False)
class Market:
  id: str = None
  type: str = None
  name: str = None
  address: str = None
  city: str = None
  postal_code: str = None
  latitude: str = None
  longitude: str = None
  last_update: date = None
  
  def dict(self):
    return asdict(self)
  
  @staticmethod
  def primary_key():
    return "id, type"

def create_table(cursor):
  columns = list(Market().dict().keys())
  columns.append(f"PRIMARY KEY ({Market.primary_key()})")
  cursor.execute(f"CREATE TABLE if not exists markets({','.join(columns)}) WITHOUT ROWID")
  return cursor

def store(markets: list[Market], path: str):
  """sqlite3. simple insert. no checking."""
  if len(markets) == 0:
    return None
  con = sqlite3.connect(path)
  cur = con.cursor()
  cur = create_table(cur)
  columns = list(Market().dict().keys())
  cur.executemany(f"REPLACE INTO markets VALUES({','.join(['?']*len(columns))})", 
                  [tuple(market.dict().values()) for market in markets])
  con.commit()
  cur.close()
  con.close()

def delete(path: str, market_type: str = None):
  """Deletes all rows in markets. Optional with market_type"""
  return None  # Dangerous! Don't do this yet.
  if not os.path.exists(path):
    return None
  con = sqlite3.connect(path)
  cur = con.cursor()
  if market_type == None:  # delete all
    sql_delete_stmt = "DELETE FROM markets"
  else:                    # delete only with where type='{market_type}'
    sql_delete_stmt = f"DELETE FROM markets where type='{market_type}'"
  cur.execute(sql_delete_stmt)
  con.commit()
  cur.close()
  con.close()

def get_last_update(market_type: str, path: str, *, get_oldest: bool = True) -> date:
  """Returns the oldest last_update for market_type."""
  if not os.path.exists(path):
    return None
  con = sqlite3.connect(path)
  cur = con.cursor()
  cur.execute(f"SELECT DISTINCT last_update FROM markets where type='{market_type}'")
  last_updates = cur.fetchall()
  last_updates = [datetime.strptime(last_update[0], "%Y-%m-%d").date() for last_update in last_updates]
  if len(last_updates) == 0:
    return None
  last_updates.sort(reverse=get_oldest)
  cur.close()
  con.close()
  return last_updates[-1]

def get_market_ids(market_type: str, path: str) -> list[str]:
  if not os.path.exists(path):
    return None
  con = sqlite3.connect(path)
  cur = con.cursor()
  cur.execute(f"SELECT id FROM markets where type='{market_type}'")
  ids = cur.fetchall()
  ids = [id[0] for id in ids]
  cur.close()
  con.close()
  return ids


if __name__ == "__main__":
  pass