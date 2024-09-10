
import os
import sqlite3
from datetime import date
from dataclasses import dataclass, asdict

group_id_markets_tablename = "group_id_markets"

@dataclass(init=False)
class Group_id_Markets:
  """Group id (e.g. selling region) together with the markets that use that group. + week_start + week_end"""
  group_id: str = None
  market_type: str = None
  markets: list | str = None
  week_start: date = None
  week_end: date = None
  last_update: date = None

  def __eq__(self, other):
    if not isinstance(other, Group_id_Markets):
      return NotImplemented
    return self.group_id == other.group_id and self.week_start == other.week_start and self.week_end == other.week_end
  
  def __hash__(self):
    return hash((self.group_id, self.week_start, self.week_end))
  
  def dict(self):
    return asdict(self)
  
  @staticmethod
  def primary_key():
    return "group_id, market_type, week_start, week_end"
  
def create_table(cursor):
  columns = list(Group_id_Markets().dict().keys())
  columns.append(f"PRIMARY KEY ({Group_id_Markets.primary_key()})")
  cursor.execute(f"CREATE TABLE if not exists {group_id_markets_tablename}({','.join(columns)}) WITHOUT ROWID")
  return cursor

def store(group_id_markets: list[Group_id_Markets], path: str):
  if len(group_id_markets) == 0:
    return None
  con = sqlite3.connect(path)
  cur = con.cursor()
  cur = create_table(cur)
  columns = list(Group_id_Markets().dict().keys())
  cur.executemany(f"REPLACE INTO {group_id_markets_tablename} VALUES({','.join(['?']*len(columns))})",
                  [tuple(group.dict().values()) for group in group_id_markets])
  con.commit()
  cur.close()
  con.close()

def get_group_ids(market_type: str, today: date, path: str) -> list[str]:
  if not os.path.exists(path):
    return None
  con = sqlite3.connect(path)
  cur = con.cursor()
  cur.execute(f"SELECT DISTINCT group_id FROM {group_id_markets_tablename} where market_type='{market_type}' and '{today}'>=week_start and '{today}'<=week_end")
  ids = cur.fetchall()
  ids = [id[0] for id in ids]
  cur.close()
  con.close()
  return ids