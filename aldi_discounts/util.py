from functools import wraps
from time import sleep
from random import randint

def delay(func):
  @wraps(func)
  def wrapper_delay(*args, **kwargs):
    s = randint(1,23) / 46
    sleep(s)
    return func(*args, **kwargs)
  return wrapper_delay