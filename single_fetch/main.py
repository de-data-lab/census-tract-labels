# dependencies
from script.single_fetch.from_address_to_tract import from_address_to_tract
from output_file_format import output_file_format

if __name__ == '__main__':
    data_file = input("Please Enter The File Name\n")
    output_name = input("Please Enter The Desired Output File Name\n")
    output_format = input("Please Chose Output Format:\n 1: for .csv\n 2: .json\n")
    if output_format == '1':
        from_address_to_tract(str(data_file), output_name)
        output_file_format('{}.json'.format(output_name), output_name)
    else:
        from_address_to_tract(str(data_file), output_name)

