import requests
from utility import save_to_json
import asyncio
import aiohttp


def list_of_urls(list_of_addresses):
    URLs_list = []
    url = "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"
    querystring = {"format": "json",
                   "address": url,
                   "benchmark": "Public_AR_Current",
                   "vintage": "Current_Current"}
    for address in list_of_addresses:
        encoded_address = requests.utils.quote(address)
        full_url = '''https://geocoding.geo.census.gov/geocoder/locations/onelineaddress?address={address}&benchmark={benchmark}&format={format}'''.format(
            address=encoded_address, benchmark=querystring['benchmark'], format=querystring['format'])
        URLs_list.append(full_url)
    return URLs_list


async def gather_with_concurrency(n, *tasks):
    semaphore = asyncio.Semaphore(n)

    async def sem_task(task):
        async with semaphore:
            return await task

    return await asyncio.gather(*(sem_task(task) for task in tasks))


async def get_async(url, session, results):
    async with session.get(url) as response:
        i = url.split('/')[-1]
        obj = await response.json()
        results.append(obj)


async def main(add):
    # https://gist.github.com/wfng92/2d2ae4385badd0f78612e447444c195f
    conn = aiohttp.TCPConnector(limit=None, ttl_dns_cache=300)
    session = aiohttp.ClientSession(connector=conn)
    results = []
    urls = add
    # number of threads
    conc_req = 40
    await gather_with_concurrency(conc_req, *[get_async(i, session, results) for i in urls])
    await session.close()
    return results


def geocode_addresses(addresses_list, output_path):
    URL_LIST = list_of_urls(addresses_list)
    run = asyncio.get_event_loop().run_until_complete(main(URL_LIST))
    save_to_json(run, output_path)

