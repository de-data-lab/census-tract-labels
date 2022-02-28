import csv
import json
from datetime import datetime
from script.single_fetch.get_lat_long_coordinates import get_lat_long_coordinates
from script.single_fetch.get_census_tract import get_census_tract


def from_address_to_tract(data_file, output_file_name):
    """
    This function will receive a data file in a csv format for address and return their census tracts infromation
    :param output_file_name: the name ot output file
    :param data_file: a .csv file that has one column with address in it
    :return: either .csv or .json file with the address and their matched information
    """
    dataFile = data_file
    file = str(dataFile)

    # reading file into list
    addresses = []
    with open(file, 'r') as f:
        csvreader = csv.reader(f)
        header = next(csvreader)
        for row in csvreader:
            addresses.append(row)

    # starting timer
    start_time = datetime.now()

    # creating output dict
    output_list = []

    # creating bathes to avoid over requesting the server
    batch_size = 5000  # has to be less than 10,000 to avoid over request error
    number_of_batches = len(addresses) // batch_size
    print('your file will be processed in {0} batches'.format(number_of_batches + 1))
    for j in range(number_of_batches + 1):
        print('Batch No: {0}'.format(j + 1) + ' ' + 'out of {0}'.format(number_of_batches + 1))
        if j != number_of_batches:
            start_batch = j * batch_size
            end_batch = (j * batch_size) + batch_size
            address_batch = addresses[start_batch:end_batch]
            for k in range(len(address_batch)):
                input_address = address_batch[k][0]
                print(input_address)
                geo_encoding = get_lat_long_coordinates(input_address)
                output_dict = {}
                if geo_encoding['matched_address'] != "Unable To Geolocate The Address":
                    census_info = get_census_tract(geo_encoding['lat'], geo_encoding['lon'])
                    output_dict['input_address'] = geo_encoding['input_address']
                    output_dict['matched_address'] = geo_encoding['matched_address']
                    output_dict['lon'] = geo_encoding['lon']
                    output_dict['lat'] = geo_encoding['lat']
                    output_dict['State'] = census_info['State']
                    output_dict['County'] = census_info['County']
                    output_dict['Census_Tract'] = census_info['Census_Tract']
                    output_dict['Block_Group'] = census_info['Block_Group']
                    output_dict['Block'] = census_info['Block']
                    output_dict['Fips_Code'] = census_info['Fips_Code']
                    output_dict['GEOID'] = census_info['GEOID']
                    output_list.append(output_dict)
                else:
                    output_dict['input_address'] = geo_encoding['input_address']
                    output_dict['matched_address'] = "No Latitude Longitude Found"
                    output_list.append(output_dict)
            start_batch = 0
            end_batch = 0
        else:  # Running the Last Batch
            start_batch = j * batch_size
            end_batch = len(addresses)
            address_batch = addresses[start_batch:end_batch]
            for k in range(len(address_batch)):
                input_address = address_batch[k][0]
                geo_encoding = get_lat_long_coordinates(input_address)
                output_dict = {}
                if geo_encoding['matched_address'] != "Unable To Geolocate The Address":
                    census_info = get_census_tract(geo_encoding['lat'], geo_encoding['lon'])
                    output_dict['input_address'] = geo_encoding['input_address']
                    output_dict['matched_address'] = geo_encoding['matched_address']
                    output_dict['lon'] = geo_encoding['lon']
                    output_dict['lat'] = geo_encoding['lat']
                    output_dict['State'] = census_info['State']
                    output_dict['County'] = census_info['County']
                    output_dict['Census_Tract'] = census_info['Census_Tract']
                    output_dict['Block_Group'] = census_info['Block_Group']
                    output_dict['Block'] = census_info['Block']
                    output_dict['Fips_Code'] = census_info['Fips_Code']
                    output_dict['GEOID'] = census_info['GEOID']
                    output_list.append(output_dict)
                else:
                    output_dict['input_address'] = geo_encoding['input_address']
                    output_dict['matched_address'] = "No Latitude Longitude Found"
                    output_list.append(output_dict)
            start_batch = 0
            end_batch = 0
    with open("{}.json".format(output_file_name), "w") as outfile:
        json.dump(output_list, outfile)
    print('----- overall time %s seconds ------' % (datetime.now() - start_time))
