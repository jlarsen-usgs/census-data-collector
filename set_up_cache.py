from censusdc.utils import census_cache_builder
import time

with open("api_key.dat") as api:
    apikey = api.readline().strip()

start_time = time.time()
census_cache_builder('block_group', apikey, multithread=False, thread_pool=11,
                     profile=False, summary=False, refresh=False)
print(time.time() - start_time)
