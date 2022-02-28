import requests
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import geocoder
import asyncio
import aiohttp


class censusTractLabel:

    def __init__(self, addresses):
        self.data = addresses

    def fuzzy_match(self, min_score=70, num_of_words=2):
        list_of_prefixes = ["east", "west", "north", "south", "street",
                            "road", "avenue", "close", "court", "boulevarde",
                            "drive", "place", "square", "parade", "circuit"]
        address = self.data
        highest = process.extract(address, list_of_prefixes, limit=num_of_words)
        for k in highest:
            address_componeents = address.split()
            for i in address_componeents:
                score = fuzz.ratio(k[0], i)
                if score > min_score:
                    word = i
                    matched_word = k[0]
                    address = address.replace(word, matched_word)
        return address

    ### step 1 Geocode an address
    def list_of_urls_census(self, list_of_addresses):
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

    async def gather_with_concurrency_census(self, n, *tasks):
        semaphore = asyncio.Semaphore(n)

        async def sem_task(task):
            async with semaphore:
                return await task

        return await asyncio.gather(*(sem_task(task) for task in tasks))

    async def get_async_census(self, url, session, results):
        async with session.get(url) as response:
            i = url.split('/')[-1]
            obj = await response.json()
            input_address = obj['result']['input']['address']['address']
            if len(obj['result']['addressMatches']) > 0:
                matched_address = obj['result']['addressMatches'][0]['matchedAddress']
                latitude = obj['result']['addressMatches'][0]['coordinates']['y']
                longitude = obj['result']['addressMatches'][0]['coordinates']['x']
                output = {'input_address': input_address,
                          'matched_address': matched_address,
                          'lat': latitude,
                          'lon': longitude,
                          'geocoding-serivce': 'census bureau'}
            else:
                matched_address = 'no matching addresses'
                latitude = '0'
                longitude = '0'
                output = {'input_address': input_address,
                          'matched_address': matched_address,
                          'lat': latitude,
                          'lon': longitude,
                          'geocoding-serivce': 'census bureau'}

            results.append(output)

    async def main_census(self, add):
        # https://gist.github.com/wfng92/2d2ae4385badd0f78612e447444c195f
        conn = aiohttp.TCPConnector(limit=None, ttl_dns_cache=300)
        session = aiohttp.ClientSession(connector=conn)
        results = []
        urls = add
        # number of threads
        conc_req = 40
        await self.gather_with_concurrency_census(conc_req, *[self.get_async_census(i, session, results) for i in urls])
        await session.close()
        return results

    def geocode_addresses(self, addresses_list):
        URL_LIST = self.list_of_urls_census(addresses_list)
        run = asyncio.get_event_loop().run_until_complete(self.main_census(URL_LIST))
        return run

    def geocode_address(self):
        geocoded_address_list = self.geocode_addresses(self.data)
        for address in geocoded_address_list:
            index = (geocoded_address_list.index(address))
            if address['matched_address'] == 'no matching addresses':
                g = geocoder.arcgis(address['input_address'])
                service_response = g.json
                # added the Delaware condition to avoid wrong parsing
                if (service_response is not None) and ('Delaware' in service_response['raw']['name']):
                    matched_address = service_response['raw']['name']
                    latitude = service_response['lat']
                    longitude = service_response['lng']
                    output = {'input_address': address['input_address'],
                              'matched_address': matched_address,
                              'lat': latitude, 'lon': longitude,
                              'geocoding-serivce': 'ArcGIS'}
                    geocoded_address_list[index] = output
                else:
                    output = {'input_address': address, 'matched_address': "Unable To Geolocate The Address"}
                    geocoded_address_list[index] = output
        return geocoded_address_list

    # step 2: get GEOID based on LAT LON output
    async def gather_with_concurrency(self, n, *tasks):
        semaphore = asyncio.Semaphore(n)

        async def sem_task(task):
            async with semaphore:
                return await task

        return await asyncio.gather(*(sem_task(task) for task in tasks))

    async def get_async(self, input, session, results):
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
                async with session.get(url, ssl=False) as response:
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

    async def main(self, addresses_list):
        conn = aiohttp.TCPConnector(limit=None, ttl_dns_cache=300)
        session = aiohttp.ClientSession(connector=conn)
        results = []
        urls = addresses_list
        # number of threads
        conc_req = 40
        await self.gather_with_concurrency(conc_req, *[self.get_async(i, session, results) for i in urls])
        await session.close()
        return results

    def census_tract_addresses(self):
        run = asyncio.get_event_loop().run_until_complete(self.main(self.geocode_address()))
        return run


#
adress_test = censusTractLabel(['93 e main st, newark, de, 19713', '93  main st, newark, de, 19713'])
print(adress_test.census_tract_addresses())
