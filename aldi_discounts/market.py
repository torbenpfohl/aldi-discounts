import sqlite3
from datetime import date, datetime
from dataclasses import dataclass, asdict

__all__ = ["Market", "store", "get_last_update"]

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
  # opening_hours: dict[str, tuple[str, str]] = None
  
  def dict(self):
    return asdict(self)


def store(markets: list[Market], path: str):
  """sqlite3. simple insert. no checking."""
  con = sqlite3.connect(path)
  cur = con.cursor()
  columns = list(Market().dict().keys())
  cur.execute(f"CREATE TABLE if not exists markets({','.join(columns)})")
  cur.executemany(f"INSERT INTO markets VALUES({','.join(['?']*len(columns))})", 
                  [tuple(market.dict().values()) for market in markets])
  con.commit()
  cur.close()
  con.close()

def get_last_update(market_type: str, path: str) -> date:
  con = sqlite3.connect(path)
  cur = con.cursor()
  cur.execute(f"SELECT DISTINCT last_update FROM markets where type='{market_type}'")
  last_updates = cur.fetchall()
  last_updates = [datetime.strptime(last_update[0], "%Y-%m-%d").date() for last_update in last_updates]
  last_updates.sort(reverse=True)
  cur.close()
  con.close()
  return last_updates[0]
  

if __name__ == "__main__":
  pass