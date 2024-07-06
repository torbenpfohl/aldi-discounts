from dataclasses import dataclass, asdict

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
  last_update: str = None
  # opening_hours: dict[str, tuple[str, str]] = None
  
  def dict(self):
    return asdict(self)



def store_markets(markets: list[Market], path: str):
  pass