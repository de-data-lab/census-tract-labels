# dependencies
import http
import http.client
import json
import ssl


def get_census_tract(latitude, longitude):
    """
    Function Description: This function receives a latitude,longitude and returns the fips code
                            and census tract number associated with the latitude and longitude
    Input: string  latitude,longitude
    Output: dict tarct_number, fips_code
    Author: Mohammad Baksh
    """


    conn = http.client.HTTPSConnection("geo.fcc.gov", context = ssl._create_unverified_context())
    payload = ''
    headers = {}
    conn.request("GET", "/api/census/block/find?lat={lat}&lon={lon}&censusYear=2021"
                 .format(lat=latitude, lon=longitude), payload, headers)
    res = conn.getresponse()
    data = res.read()
    Data = json.loads(data.decode("utf-8"))
    results = Data['results']
    fips_code = str(results[0]['block_fips'])  # state(2)County(3)Tract(6)Block(4) total 15
    state = fips_code[0:2]
    County = fips_code[2:5]
    Census_Tract = fips_code[5:11]
    Block_Group = fips_code[11]
    Block = fips_code[11:15]
    geo_id = fips_code[0:11]  # state(2)County(3)Tract(6) total 11
    output_dict = {'State': state,
                   'County': County,
                   'Census_Tract': Census_Tract,
                   'Block_Group': Block_Group,
                   'Block': Block,
                   'Fips_Code': fips_code,
                   'GEOID': geo_id}
    return output_dict
