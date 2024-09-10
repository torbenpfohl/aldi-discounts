"""Extra market infos. 

Can contain empty fields, since not all information are available for all market types"""

import os
import sqlite3
from datetime import date
from datetime import time
from dataclasses import dataclass, asdict

market_extra_tablename = "market_extra"

@dataclass(init=False)
class Market_Extra:
  id: str = None
  type: str = None
  self_checkout: bool = None
  renovation_start: date = None
  reopen: date = None
  temporarily_closed_start: date = None
  temporarily_closed_end: date = None
  weekday_0_open: bool = None
  weekday_0_start: time = None
  weekday_0_end: time = None
  last_update: date = None
  # ...

  def dict(self):
    return asdict(self)
  
  @staticmethod
  def primary_key():
    return "id, type"

def create_table(cursor):
  columns = list(Market_Extra().dict().keys())
  columns.append(f"PRIMARY KEY ({Market_Extra.primary_key()})")
  cursor.execute(f"CREATE TABLE if not exists {market_extra_tablename}({','.join(columns)}) WITHOUT ROWID")
  return cursor

def store(markets_extra: list[Market_Extra], path: str):
  if len(markets_extra) == 0:
    return None
  con = sqlite3.connect(path)
  cur = con.cursor()
  cur = create_table(cur)
  columns = list(Market_Extra().dict().keys())
  cur.executemany(f"REPLACE INTO {market_extra_tablename} VALUES({','.join(['?']*len(columns))})", 
                  [tuple(market.dict().values()) for market in markets_extra])
  con.commit()
  cur.close()
  con.close()