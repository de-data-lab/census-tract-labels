# dependencies
import requests
import geocoder
import aiohttp
import asyncio

def geocoding_using_census(address):
    """
    Function Description: This function uses the Geocoder - United States Census Bureau API
                            latency of the response is 1,303ms per request
    :param address:  string address
    :return:  dict {'input input_address', 'matched address','lat', 'lon'}
    :Author: Mohammad Baksh
    """
    url = "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"

    querystring = {"format": "json", "address": address, "benchmark": "Public_AR_Current",
                   "vintage": "Current_Current"}

    headers = {}

    response = requests.request("GET", url, headers=headers, params=querystring)
    response_data = response.json()
    response_data_res = response_data['result']
    response_address_match = response_data_res['addressMatches']
    if len(response_address_match) > 0:
        response_address_match_address = response_address_match[0]['matchedAddress']
        response_address_match_coordinate = response_address_match[0]['coordinates']
        output = {'input_address': address, 'matched_address': response_address_match_address,
                  'lat': response_address_match_coordinate['y'], 'lon': response_address_match_coordinate['x']}
        return output
    else:
        output = {'input_address': address, 'matched_address': "Unable To Geolocate The Address"}
        return output


def geocoding_using_geocoder_lib(address):
    """
    Function Description: This function uses the geocoder library to geocoded addresses
                            https://github.com/DenisCarriere/geocoder
                            using ArcGIS service limit at 20000 request daily
                            ArcGIS latency of the response is 87ms per request
    :param address:  string address
    :return:  dict {'input input_address', 'matched address','lat', 'lon'}
    :Author: Mohammad Baksh
    """
    g = geocoder.arcgis(address)
    service_response = g.json

    # added the Delaware condition to avoid wrong parsing
    if (service_response is not None) and ('Delaware' in service_response['raw']['name']):
        matched_address = service_response['raw']['name']
        latitude = service_response['lat']
        longitude = service_response['lng']
        output = {'input_address': address, 'matched_address': matched_address, 'lat': latitude, 'lon': longitude}
        return output
    else:
        output = {'input_address': address, 'matched_address': "Unable To Geolocate The Address"}
        return output


def get_lat_long_coordinates(address):
    """
     Function Description: This function receives a single line address in the form of number street,city, state,
     zip code and returns the latitude and longitude associated with the input address
    :param address:  string address
    :return:  dict {'input input_address', 'matched address','lat', 'lon'}
    :Author: Mohammad Baksh
    """
    geccodded_address = geocoding_using_census(address)
    if geccodded_address['matched_address'] == "Unable To Geolocate The Address":
        return geocoding_using_geocoder_lib(address)
    else:
        return geccodded_address



if __name__ == '__main__':
    address = "5 okie dr, landenberg, de, 19707"
    address2 = "95 E main st, newark de"
    print(geocoding_using_census(address))
    # print(geocoding_using_geocoder_lib(address))
    # print(geocoding_using_geocoder_lib(address2))