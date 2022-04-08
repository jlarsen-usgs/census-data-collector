from censusdc.utils import census_cache_builder
import time

with open("api_key.dat") as api:
    apikey = api.readline().strip()

start_time = time.time()
census_cache_builder('place', apikey, multithread=True, thread_pool=11,
                     profile=True, summary=False, refresh=True)
print(time.time() - start_time)
