
### very much a work in progress

# TODOS
- [ ] APIs vereinheitlichen
  - [ ] eigene logger. die außer kraft gesetzt werden können.
  - [ ] logging an den gleichen stellen. prüfen was sinn ergibt: debug, info, warning, ...
- [ ] Threading
- [ ] Progressbar


#### What happens on a sunday? (But what happens around midnight?)
- penny: uses the calendar-week in the request, but the extra_details urls are returning 404 a lot (all throughout) on sunday for the past week
- rewe: products for the next week..
- aldi_sued: products for the next week. the date-based urls (partial week) are available still. (sometimes prices can be missing)
  - in fruit & vegetable category ("frischekracher") the prices for the next week are not available before saturday 7 am
  - for the partial-week-urls: on sunday we get the next week, i.e. the next day (monday), ...
- aldi_nord: products for the next week only.
- hit: ???
 > preliminary conclusion: on sundays use the next week as target. 

#### What to do, if requests span saturday and sunday?
- give an estimated for all requests and warn?
- start over on sunday?


----

makes a lot of assumptions regarding the structur of the returned and parsed website- and json-data.. 

----

#### Dependencies:

- httpx[http2]
- cryptography

## Code Structur
- main.py does all the main database work.
- marketlists.py mostly calls the endpoints (rewe owns a key- and a pem-file for authentication)
  - every market-class exposes a method **get_markets()** which returns markets (or at least an empty list) or - if something went wrong and we should abort this market-retrieval - it returns None
  - every market-class owns a logger (e.g. marketlists.Penny), that _can_ propagate its messages to the marketlists-logger
- discounts.py SHOULD also mostly call the endpoints (same auth stuff needed), it exposes a function **get_products()** which takes ___nothing___ or one or multiple ___market id___(s) - or a ___selling region___ in some cases - and returns the products for that market/ selling region
- market_products.py, market.py, product.py, market_extra.py are all dataclasses that also have some storage-functions
- util.py for useful/necessary functions


## Data Structur

- one table for all markets.
- one table for all products.
- intermediary tables to safe space / minimize the number of rows in product table

   - REWE: one table, in which one row contains a rewe-market id and the ids of the offers. (so no need to store all offers multiple times in detail)
   - HIT: Same as REWE.
   - PENNY: two tables: 
  
     1. one row contains a selling region and the offer ids for that selling region.
     2. (extra market infos table) one row contains a market id and extra infos for that market, like opening times - but also a column with the selling region for that market. 

 - all tables - besides the table with the markets - have two mandatory columns: 
   - the week start and (monday)
   - the week end (saturday)