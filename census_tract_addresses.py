import asyncio
import aiohttp


async def gather_with_concurrency(n, *tasks):
    semaphore = asyncio.Semaphore(n)

    async def sem_task(task):
        async with semaphore:
            return await task

    return await asyncio.gather(*(sem_task(task) for task in tasks))


async def get_async(input, session, results):
    input_address = input['input_address']
    geocodded_addrese = input['matched_address']
    lat = input['lat']
    lon = input['lon']
    if geocodded_addrese != "Unable To Geolocate The Address":
        lat = str(lat)
        lon = str(lon)
    else:
        lat = str(0)
        lon = str(0)

    url = '''https://geo.fcc.gov/api/census/block/find?latitude={lat}&longitude={lon}&showall=false&format=json'''.format(
            lat=lat, lon=lon)
    data = None
    while data is None:
        try:
            async with session.get(url,ssl=False) as response:
                result_data = await response.json()
                data = result_data
                fips_code = data['Block']['FIPS']
                if fips_code != None:
                    fips_code = data['Block']['FIPS']
                    Census_Tract = fips_code[5:11]
                    Block_Group = fips_code[11]
                    Block = fips_code[11:15]
                    geo_id = fips_code[0:11]  # state(2)County(3)Tract(6) total 11
                else:
                    geocodded_addrese = 'Unable To Geolocate The Address'
                    fips_code = 0
                    Census_Tract = 0
                    Block_Group = 0
                    Block = 0
                    geo_id = 0

                output = {'input addresses': input_address,
                          'matched addresses': geocodded_addrese,
                          'lot': lat,
                          'lat': lon,
                          'state': data['State']['code'],
                          'county': data['County']['name'],
                          'census tract': Census_Tract,
                          'block group': Block_Group,
                          'block': Block,
                          'FIPS': data['Block']['FIPS'],
                          'GEOID': geo_id
                          }
                results.append(output)
        except aiohttp.ClientError:
            # sleep a little and try again
            await asyncio.sleep(1)



async def main(add):
    conn = aiohttp.TCPConnector(limit=None, ttl_dns_cache=300)
    session = aiohttp.ClientSession(connector=conn)
    results = []
    urls = add
    # number of threads
    conc_req = 40
    await gather_with_concurrency(conc_req, *[get_async(i, session, results) for i in urls])
    await session.close()
    return results


def FCC_API_parallel(addresses_list):
    run = asyncio.get_event_loop().run_until_complete(main(addresses_list))
    return run
