class Product:

  def __init__(self):
    self._name = ""
    self._price = ""
    #self._price_base = ""
    #self._price_base_unit = ""
    self._price_before = ""
    self._producer = ""
    self._description = ""
    self._currency = "€"
    self._valid_from = ""
    self._valid_to = ""
    self._link = ""
    #self._origin = ""
    #self._unique_id = ""  # only unique in the context of the respective market

  def __str__() -> str:
    pass

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