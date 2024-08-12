import re
import os
import shutil
import logging
from pathlib import Path
from random import randint
from functools import wraps
from zipfile import ZipFile
from datetime import date, timedelta
from time import sleep, struct_time
from cryptography.hazmat.primitives.serialization import Encoding, PrivateFormat, NoEncryption, pkcs12

from httpx import Client

APK_FILE = "rewe.apk"
APK_DIR = "rewe"
MTLS_PROD = "mtls_prod.pfx"
MTLS_PASSWORD = b"NC3hDTstMX9waPPV"

def get_rewe_creds(source_path, key_filename, cert_filename):
  """Fetch the apk and extract private key and certificate.

  APK source: uptodown.com
  """
  # Get the apk. 
  FULL_APK_DIR_PATH = os.path.join(source_path, APK_DIR)
  FULL_APK_FILE_PATH = os.path.join(source_path, APK_FILE)
  FULL_MTLS_PROD_PATH = os.path.join(source_path, MTLS_PROD)
  FULL_KEY_FILE_PATH = os.path.join(source_path, key_filename)
  FULL_CERT_FILE_PATH = os.path.join(source_path, cert_filename)

  print("Starting to fetch the private.key and private.pem. This could take a moment.")
  headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7", 
    "Accept-Encoding": "gzip, deflate, br, zstd", 
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Accept-Language": "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7,fr;q=0.6,it;q=0.5,nb;q=0.4,no;q=0.3",
    "Dnt": "1",
    "Priority": "u=0, i",
    }
  with Client(http2=True) as client:
    client.headers = headers

    url = "https://rewe.de.uptodown.com/android/download"
    res = client.get(url)
    pattern = r"data-url=\".+?\""
    all_data_patterns = re.findall(pattern, res.text)
    if all_data_patterns == None or len(all_data_patterns) == 0:
      return "error: couldn't find data patterns."
    all_data_patterns.sort(reverse=True, key=lambda x: len(x))
    the_one = all_data_patterns[0]
    the_one = the_one.strip("data-url=").strip("\"")

    url2 = "https://dw.uptodown.com/dwn/" + the_one
    res2 = client.get(url2)
    if res2.status_code == 302:
      url3 = res2.headers["location"]
      print("Download the rewe.apk.")
      res3 = client.get(url3)
      with open(FULL_APK_FILE_PATH, "wb") as file:
        file.write(res3.content)
    else:
      return f"error: unexpected status code: {res2.status_code}"

  # Unpack the apk and get mtls_prod.pfx.
  with ZipFile(FULL_APK_FILE_PATH, "r") as zipfile:
    zipfile.extractall(FULL_APK_DIR_PATH)

  print("Search for the pfx-file that holds the private key and certificate.")
  for root, _, files in os.walk(FULL_APK_DIR_PATH):
    if "mtls_prod.pfx" in files:
      mtls_path = os.path.join(root, "mtls_prod.pfx")
      os.rename(mtls_path, FULL_MTLS_PROD_PATH)
  shutil.rmtree(FULL_APK_DIR_PATH)
  os.remove(FULL_APK_FILE_PATH)

  # Split into private key and certificate.
  with open(FULL_MTLS_PROD_PATH, "rb") as pfx_file:
    private_key, certificate, _ = pkcs12.load_key_and_certificates(data=pfx_file.read(), password=MTLS_PASSWORD)

  key = private_key.private_bytes(encoding=Encoding.PEM, format=PrivateFormat.PKCS8, encryption_algorithm=NoEncryption())
  cert = certificate.public_bytes(encoding=Encoding.PEM)

  with open(FULL_KEY_FILE_PATH, "wb") as key_file:
    key_file.write(key)
  with open(FULL_CERT_FILE_PATH, "wb") as cert_file:
    cert_file.write(cert)

  os.remove(FULL_MTLS_PROD_PATH)
  print("Finished.")
  return None


def delay_range(min_msec: int = 200, max_msec: int = 1000):
  """Add delay between min_msec-milliseconds and max_msec-milliseconds."""
  def delay(func):
    @wraps(func)
    def wrapper_delay(*args, **kwargs):
      s = randint(min_msec, max_msec)
      sleep(s / 1000)
      return func(*args, **kwargs)
    return wrapper_delay
  return delay


def set_week_start_and_end(now: struct_time) -> tuple[date, date]:
  """week_start = monday; week_end = sunday."""
  now_date = date(now.tm_year, now.tm_mon, now.tm_mday)
  # week_start = now_date - timedelta(days=now.tm_wday)
  # week_end = now_date + timedelta(days=(6 - now.tm_wday))

  if now.tm_wday == 6:  # sunday
   week_start = now_date + timedelta(days=1)
   week_end = now_date + timedelta(days=6)
  else:
   week_start = now_date - timedelta(days=now.tm_wday)
   week_end = now_date + timedelta(days=(5 - now.tm_wday))

  return week_start, week_end


def setup_logger(source_path: str, parent_name: str, logger_name: str, loglevel: str, one_logger_file: bool, filemode: str) -> logging.Logger:
  LOG_PATH_FOLDERNAME = "log"
  if not (os.path.exists(LOG_PATH := os.path.join(source_path, LOG_PATH_FOLDERNAME))):
    os.mkdir(LOG_PATH)

  logger_name = ".".join([parent_name, logger_name])
  logger = logging.getLogger(logger_name)
  logger.setLevel(loglevel)
  if one_logger_file:
    log_handler = logging.FileHandler(os.path.join(LOG_PATH, f"{logger_name}.log"), mode=filemode, encoding="utf-8")
    log_formatter = logging.Formatter("%(name)s (%(levelname)s) - %(asctime)s - %(message)s")
    log_handler.setFormatter(log_formatter)
    logger.addHandler(log_handler)

  return logger


"""
import sqlite3
from market import Market

def report_rewe_db():
  m_path = "./manuell-backup/markets.db"
  new_m_path = "./markets.db"
  
  # connect to old / get data
  # con = sqlite3.connect(m_path)
  # cur = con.cursor()
  # cur.execute("select * from markets where type='rewe'")
  # all_ms = cur.fetchall()
  # print(len(all_ms))
  # print(all_ms[0])
  # cur.execute("select count(id) from markets where type='rewe'")
  # all = cur.fetchall()
  # print(all)
  # cur.execute("select distinct count(id) from markets where type='rewe'")
  # dist = cur.fetchall()
  # print(dist)
  # cur.close()
  # con.close()

  # connect to new
  con2 = sqlite3.connect(new_m_path)
  cur2 = con2.cursor()
  columns = list(Market().dict().keys())
  columns.append(f"PRIMARY KEY ({Market.primary_key()})")
  print(columns)
  cur2.execute(f"CREATE TABLE if not exists markets({','.join(columns)}) WITHOUT ROWID")
  for ms in [('561157', 'rewe', 'REWE Krause oHG', 'Lübecker Straße 68', 'Lüneburg', '21337', '53.25334', '10.4316', '2024-07-14')]:
    try:
      cur2.execute("replace into markets values(?,?,?,?,?,?,?,?,?)", ms)
      con2.commit()
    except:
      print(ms)
      continue
  cur2.close()
  con2.close()
"""




if __name__ == "__main__":
  source_path = Path(__file__).resolve().parent
  if not os.path.exists(tmp_path := os.path.join(source_path, "tmp")):
    os.mkdir(tmp_path)
  get_rewe_creds(tmp_path, "private_test.key", "private_test.pem")