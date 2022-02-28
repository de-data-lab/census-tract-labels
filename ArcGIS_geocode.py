import geocoder
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
