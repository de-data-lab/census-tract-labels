import csv
import json
import os
import io
from parallael_fetch.ArcGIS_geocode import geocoding_using_geocoder_lib
from parallael_fetch.address_cleaning import fuzzy_matching


def read_csv(path):
    if os.path.exists(path):
        file = open(path, "r")
        csv_reader = csv.reader(file)
        lists_from_csv = []
        for row in csv_reader:
            lists_from_csv.append(row)
        return lists_from_csv
    else:
        print('file does not exist')


def save_dict_to_csv(headers, data, path):
    if os.path.exists("{}.csv".format(path)):
        try:
            with open("{}.csv".format(path), "a", newline="") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=headers)
                writer.writeheader()
                for key in data:
                    writer.writerow(key)
        except IOError:
            print("I/O error")

    else:
        try:
            with open("{}.csv".format(path), "w", newline="") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=headers)
                writer.writeheader()
                for key in data:
                    writer.writerow(key)
        except IOError:
            print("I/O error")


def save_list_to_csv(data, path):
    if os.path.exists("{}.csv".format(path)):
        try:
            with open("{}.csv".format(path), "a", newline="") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows([data])
        except IOError:
            print("I/O error")

    else:
        try:
            with open("{}.csv".format(path), "w", newline="") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows([data])
        except IOError:
            print("I/O error")


def read_json(path):
    data = []
    for line in open(path, 'r'):
        data.append(json.loads(line))
    return data


def save_to_json(data, path):
    if os.path.exists('{}.json'.format(path)):
        file = open('{}.json'.format(path), "r+")
        file_data = json.loads(file)
        #file_data = json.JSONDecoder(file)
        file_data.append(data)
        file.seek(0)
        json.dump(file_data, file, indent=4)
        file.close()
    else:
        file = open('{}.json'.format(path), "w")
        data = [data]
        json.dump(data, file)
        file.close()
    pass


def process_geocoded_address(path):
    data = read_json(path)
    output_list = []

    for i in range(len(data[0][0])):
        output_dict = {}
        # print(data[0][0][i])
        if 'exceptions' in data[0][0][i]:
            pass
            # print('exception occur')
        else:
            input_address = data[0][0][i]['result']['input']['address']['address']
            matching_results = data[0][0][i]['result']['addressMatches']
            if len(matching_results) > 0:
                matched_address = data[0][0][i]['result']['addressMatches'][0]['matchedAddress']
                lat = data[0][0][i]['result']['addressMatches'][0]['coordinates']['y']
                lon = data[0][0][i]['result']['addressMatches'][0]['coordinates']['x']
            else:
                matched_address = "Unable To Geolocate The Address"
                lat = 0
                lon = 0
            output_dict['input_address'] = input_address
            output_dict['matched_address'] = matched_address
            output_dict['lon'] = lon
            output_dict['lat'] = lat
            output_list.append(output_dict)
    return output_list


def get_diffrance_between_lists(input_data, geocodec_data):
    geocodec_addresses = []
    # added block to get list if inout addresses
    for i in range(len(geocodec_data)):
        if (geocodec_data[i]['matched_address']!='Unable To Geolocate The Address'):
            geocodec_addresses.append(geocodec_data[i]['input_address'])

    diff = list(set(input_data) - set(geocodec_addresses))
    return diff

