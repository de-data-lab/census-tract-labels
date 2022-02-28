import json
import csv

def output_file_format(input_file, output_file_name):
    """
    This function receives an input of .json file and convert it either to .txt or .csv file
    :param file: .json file
    :return: .txt or .csv file
    """

    # Open the JSON file & load its data
    with open(input_file) as dat_file:
        data = json.load(dat_file)

    keys = data[0].keys()
    with open('{0}.csv'.format(output_file_name), 'w', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(data)

if __name__ == '__main__':
    file = 'output.json'
    ou = 'addresses'
    output_file_format(file, ou)
