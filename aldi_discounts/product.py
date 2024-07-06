
from dataclasses import dataclass, asdict
# TODO: use @dataclass

@dataclass(init=False)
class Product:
  name: str = None
  price: str = None
  quantity: str = None
  base_price: str = None   # e.g. 9.95
  base_price_unit: str = None   # e.g. 1 kg
  price_before: str = None
  producer: str = None
  description: str = None
  currency: str = "€"
  valid_from: str = None
  valid_to: str = None
  link: str = None
  origin: str = None
  unique_id: str = None
  app_deal: bool = None

  def __eq__(self, other):
    return self.unique_id == other.unique_id
  
  def __hash__(self):
    return hash(self.unique_id)


class Product2:

  def __init__(self):
    self._name = ""
    self._price = ""
    #self._price_base = ""  # e.g. 320
    #self._price_base_unit = ""  # e.g. g
    self._price_before = ""
    self._producer = ""
    self._description = ""
    self._currency = "€"
    self._valid_from = ""
    self._valid_to = ""
    self._link = ""
    self._origin = ""
    self._unique_id = ""  # only unique in the context of the respective market
    #self._nutri_score = ""
    #self._app_deal = False

  def __str__() -> str:
    pass

  def __eq__(self, other):
    return self.unique_id == other.unique_id

  def __hash__(self):
    return hash(self.unique_id)

  @property
  def name(self):
    return self._name
  
  @name.setter
  def name(self, name):
    self._name = name

  @property
  def price(self):
    return self._price
  
  @price.setter
  def price(self, price):
    self._price = str(price).replace('.', ',')

  @property
  def price_before(self):
    return self._price
  
  @price_before.setter
  def price_before(self, price):
    self._price_before = str(price).replace('.', ',')

  @property
  def producer(self):
    return self._producer
  
  @producer.setter
  def producer(self, producer):
    self._producer = producer
  
  @property
  def description(self):
    return self._description
  
  @description.setter
  def description(self, description):
    self._description = description
  
  @property
  def valid_from(self):
    return self._valid_from
  
  @valid_from.setter
  def valid_from(self, valid_from):
    self._valid_from = valid_from

  @property
  def valid_to(self):
    return self._valid_to
  
  @valid_to.setter
  def valid_to(self, valid_to):
    self._valid_to = valid_to

  @property
  def link(self):
    return self._link
  
  @link.setter
  def link(self, link):
    self._link = link

  @property
  def unique_id(self):
    return self._unique_id

  @unique_id.setter
  def unique_id(self, unique_id):
    self._unique_id = unique_id

  @property
  def origin(self):
    return self._origin

  @origin.setter
  def origin(self, origin):
    self._origin = origin


def store_products(products: list[Product], path: str):
  pass