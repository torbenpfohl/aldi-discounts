Create a wrapper, but it into a util.py-file, and use it wherever we make api-calls with no rate limit.
(move get_creds-function into util.py as well)

one function to coordinate the creation of the marketlists 
--> use one store_markets()-function for all storing? store markets together in one database?
one function to coordinate the product retrival 
--> they both need a lot of exception handling.

-> change to httpx and http 2 for all requests