import os
import time
import pandas as pd
from address_cleaning import fuzzy_matching
from census_tract_addresses import FCC_API_parallel
from geocode_address import geocode_addresses
from parallael_fetch.ArcGIS_geocode import geocoding_using_geocoder_lib
from utility import read_csv, process_geocoded_address, get_diffrance_between_lists, save_dict_to_csv, save_list_to_csv


def main(paramters):
    now = time.time()
    # reading data IN
    cureent_dir = os.getcwd()
    # creating output file
    if os.path.exists(os.path.join(cureent_dir, 'output')) == False:
        os.mkdir(os.path.join(cureent_dir, 'output'))
    data_path = os.path.join(os.path.join(cureent_dir, 'data'), paramters['Data_Path'])
    DATA = read_csv(data_path)
    DATA.pop(0)  # remove the header
    list_of_add = []
    for i in range(len(DATA)):
        list_of_add.append(DATA[i][0])
    # fuzzy matching
    fuzzy_matched_addressses = []
    time_taken_1 = 0
    if paramters['fuzzy_matching']['run_fuzzy_matching']:
        for address in DATA:
            dic = {}
            temp = fuzzy_matching(address[0],
                                  min_score=paramters['fuzzy_matching']['fuzzy_matching_min_score'],
                                  num_of_words=paramters['fuzzy_matching']['fuzzy_matching_num_of_words'])
            dic['input_address'] = address[0]
            dic['fuzzy_matched_address'] = temp
            fuzzy_matched_addressses.append(dic)
        # save the output file
        fuzzy_matched_addresses_output = os.path.join(cureent_dir, 'output', paramters['fuzzy_matching']['output_path'])
        file_headers = ['input_address', 'fuzzy_matched_address']
        save_dict_to_csv(file_headers, fuzzy_matched_addressses, fuzzy_matched_addresses_output)
        time_taken_1 = time.time() - now
        print('fuzzy matching took{t} seconds'.format(t=time_taken_1))
    #####  geocode the addresses ####
    # Census Bureau API
    time_taken_2 = 0
    if paramters['geocode_service_census_bureau']['use_census_bureau_API']:
        if paramters['fuzzy_matching']['run_fuzzy_matching']:
            input_data = []
            for i in range(len(fuzzy_matched_addressses)):
                input_data.append(fuzzy_matched_addressses[i]['fuzzy_matched_address'])
        else:
            input_data = list_of_add
        json_intermedite_file = os.path.join(cureent_dir, 'output',
                                             paramters['geocode_service_census_bureau']['intermediate_file_path'])
        if os.path.exists(json_intermedite_file + '.json'):
            os.remove(json_intermedite_file + '.json')
        # print(input_data)
        geocode_addresses(input_data, json_intermedite_file)
        time_taken_2 = time.time() - (now + time_taken_1)
        print('__________________')
        print('Geocoding via API 1 took {t} seconds'.format(t=time_taken_2))
        load_intermediate_file = process_geocoded_address(json_intermedite_file + '.json')
        # print(load_intermediate_file)
        if paramters['geocode_service_census_bureau']['save_output']:
            file_path = os.path.join(cureent_dir, 'output', paramters['geocode_service_census_bureau']['output_path'])
            file_headers = ['input_address', 'matched_address', 'lat', 'lon']
            save_dict_to_csv(file_headers, load_intermediate_file, file_path)
    # ArchGIS
    time_taken_3 = 0
    geocodec_list = []
    if paramters['geocode_service_ARC_GIS']['use_ArcGIS_API']:
        if paramters['fuzzy_matching']['run_fuzzy_matching']:
            fuazzy_addresses = []
            fuzzy_matched_addresses_path = os.path.join(cureent_dir, 'output',
                                                        paramters['fuzzy_matching']['output_path'] + '.csv')
            load_fuzzzy_addresses = read_csv(fuzzy_matched_addresses_path)
            for i in range(len(load_fuzzzy_addresses)):
                if i == 0:
                    pass
                else:
                    fuazzy_addresses.append(load_fuzzzy_addresses[i][1])  # 1 correspond to fuzzy matched
            geocodded_addresses = []
            if paramters['geocode_service_census_bureau']['save_output']:
                path_to_geocoded_data = os.path.join(cureent_dir,
                                                     'output',
                                                     paramters['geocode_service_census_bureau']['output_path'] + '.csv')
                load_file = read_csv(path_to_geocoded_data)
                for i in range(len(load_file)):
                    if i == 0:
                        pass
                    else:

                        if load_file[i][1] != 'Unable To Geolocate The Address':  # 1 is the col of matched address
                            geocodded_addresses.append(load_file[i][0])  # 0 is the col of input address
            else:
                path_to_geocoded_data = os.path.join(cureent_dir, 'output',
                                                     paramters['geocode_service_census_bureau'][
                                                         'intermediate_file_path'])
                load_intermediate_file = process_geocoded_address(path_to_geocoded_data + '.json')
                for i in range(len(load_intermediate_file)):
                    if load_intermediate_file[i]['matched_address'] != 'Unable To Geolocate The Address':
                        geocodded_addresses.append(load_intermediate_file[i]['input_address'])
            diff = list(set(fuazzy_addresses) - set(geocodded_addresses))
        else:  # NO FUZZY
            geocodded_addresses = []
            if paramters['geocode_service_census_bureau']['save_output']:
                path_to_geocoded_data = os.path.join(cureent_dir,
                                                     'output',
                                                     paramters['geocode_service_census_bureau']['output_path'] + '.csv')
                load_file = read_csv(path_to_geocoded_data)
                for i in range(len(load_file)):
                    if i == 0:
                        pass
                    else:

                        if load_file[i][1] != 'Unable To Geolocate The Address':  # 1 is the col of matched address
                            geocodded_addresses.append(load_file[i][0])  # 0 is the col of input address
            else:
                path_to_geocoded_data = os.path.join(cureent_dir, 'output',
                                                     paramters['geocode_service_census_bureau'][
                                                         'intermediate_file_path'])
                load_intermediate_file = process_geocoded_address(path_to_geocoded_data + '.json')
                for i in range(len(load_intermediate_file)):
                    if load_intermediate_file[i]['matched_address'] != 'Unable To Geolocate The Address':
                        geocodded_addresses.append(load_intermediate_file[i]['input_address'])
            diff = list(set(list_of_add) - set(geocodded_addresses))
        if paramters['geocode_service_ARC_GIS']['save_differance_list']:
            diff_list_path = os.path.join(cureent_dir, 'output'
                                          , paramters['geocode_service_ARC_GIS']['differance_list_path'])
            save_list_to_csv(diff, diff_list_path)
        diff_lsit = diff
        arcgis_list = []
        for i in range(len(diff_lsit)):
            geocoded_address = geocoding_using_geocoder_lib(diff_lsit[i])
            arcgis_list.append(geocoded_address)

        if paramters['geocode_service_ARC_GIS']['save_output']:
            arcgis_file_path = os.path.join(cureent_dir, 'output'
                                            , paramters['geocode_service_ARC_GIS']['output_path'])
            file_headers = ['input_address', 'matched_address', 'lon', 'lat']
            save_dict_to_csv(file_headers, arcgis_list, arcgis_file_path)
        time_taken_3 = time.time() - (now + time_taken_2 + time_taken_1)
        print('__________________')
        print('Geocoding via API 2 took {t} seconds'.format(t=time_taken_3))
        # merge files togather
        if paramters['fuzzy_matching']['run_fuzzy_matching']:
            if paramters['geocode_service_census_bureau']['save_output'] and paramters['geocode_service_ARC_GIS'][
                'save_output']:
                fuzzy_matching_path = os.path.join(cureent_dir, 'output',
                                                   paramters['fuzzy_matching']['output_path'] + '.csv')
                cencus_buearu_path = os.path.join(cureent_dir, 'output',
                                                  paramters['geocode_service_census_bureau'][
                                                      'output_path'] + '.csv')
                arcgis_file_path = os.path.join(cureent_dir, 'output'
                                                , paramters['geocode_service_ARC_GIS']['output_path'] + '.csv')
                addr1 = pd.read_csv(cencus_buearu_path, header=0)
                addr2 = pd.read_csv(arcgis_file_path, header=0)
                addr = pd.concat([addr1, addr2])
                addr = addr[addr['matched_address'] != 'Unable To Geolocate The Address']
                addr = addr.groupby('input_address').head(1)
                fuzzy = pd.read_csv(fuzzy_matching_path, header=0)
                addr = pd.merge(addr, fuzzy, left_on='input_address', right_on='fuzzy_matched_address', how='right')
                addr = addr[['input_address_y', 'matched_address', 'lon', 'lat']]
                addr = addr.rename(columns={'input_address_y': 'input_address'})
                addr.lat.fillna(0, inplace=True)
                addr.lon.fillna(0, inplace=True)
                addr.matched_address.fillna('Unable To Geolocate The Address', inplace=True)
                geocodec_list = addr.to_dict('records')
            else:
                print('parameters error found')
        else:
            if paramters['geocode_service_census_bureau']['save_output'] and paramters['geocode_service_ARC_GIS'][
                'save_output']:
                cencus_buearu_path = os.path.join(cureent_dir, 'output',
                                                  paramters['geocode_service_census_bureau'][
                                                      'output_path'] + '.csv')
                arcgis_file_path = os.path.join(cureent_dir, 'output'
                                                , paramters['geocode_service_ARC_GIS']['output_path'] + '.csv')
                addr1 = pd.read_csv(cencus_buearu_path, header=0)
                addr2 = pd.read_csv(arcgis_file_path, header=0)
                addr = pd.concat([addr1, addr2])
                addr = addr[addr['matched_address'] != 'Unable To Geolocate The Address']
                addr = addr.groupby('input_address').head(1)
                addr.lat.fillna(0, inplace=True)
                addr.lon.fillna(0, inplace=True)
                addr.matched_address.fillna('Unable To Geolocate The Address', inplace=True)
                geocodec_list = addr.to_dict('records')
            else:
                print('parameters error found')

    # census tract addresses
    time_taken_4 = 0
    if paramters['geocode_service_ARC_GIS']['use_ArcGIS_API']:
        addresses_census_tracked = FCC_API_parallel(geocodec_list)
        addresses_census_tracked_output = os.path.join(cureent_dir, 'output',
                                                       paramters['addresses_census_tracked_path'])
        file_headers = ['input addresses', 'matched addresses', 'lot', 'lat', 'state',
                        'county', 'census tract', 'block group', 'block', 'FIPS', 'GEOID']
        save_dict_to_csv(file_headers, addresses_census_tracked, addresses_census_tracked_output)
        time_taken_4 = time.time() - (now + time_taken_4)
        print('__________________')
        print('Tract Labeling took {t} seconds'.format(t=time_taken_3))
    else:
        addresses_census_tracked = FCC_API_parallel(load_intermediate_file)
        addresses_census_tracked_output = os.path.join(cureent_dir, 'output',
                                                       paramters['addresses_census_tracked_path'])
        file_headers = ['input addresses', 'matched addresses', 'lot', 'lat', 'state',
                        'county', 'census tract', 'block group', 'block', 'FIPS', 'GEOID']
        save_dict_to_csv(file_headers, addresses_census_tracked, addresses_census_tracked_output)
        time_taken_4 = time.time() - (now + time_taken_3 + time_taken_2 + time_taken_1)
        print('__________________')
        print('Tract Labeling took {t} seconds'.format(t=time_taken_4))
    # merge back to ME table
    time_taken_5 = 0
    if paramters['merge_to_me_database']['merge_orig']:
        census_tracted_addresses_path = os.path.join(cureent_dir,
                                                     'output',
                                                     paramters['addresses_census_tracked_path'])
        tracted = pd.read_csv(census_tracted_addresses_path + '.csv', header=0)
        tracted = tracted.rename(columns={'input address': 'priority_address'})
        data_path = os.path.join(os.path.join(cureent_dir, 'data'), paramters['Orig_Path'])
        sample = pd.read_csv(data_path, header=0)
        final_output = pd.merge(sample, tracted, how='left', on='priority_address')
        final_output.to_csv('final_output.csv', index=False)
        time_taken_5 = time.time() - (now + time_taken_4)
        print('__________________')
        print('Merge to ME Data Took {t} seconds'.format(t=time_taken_5))


paramters = {'Data_Path': 'meAddr.csv',
             'fuzzy_matching': {'run_fuzzy_matching': False,
                                'fuzzy_matching_min_score': 70,
                                'fuzzy_matching_num_of_words': 2,
                                'output_path': 'fuzzy_matching_output'},

             'geocode_service_census_bureau': {'use_census_bureau_API': True,
                                               'intermediate_file_path': 'json_file',
                                               'save_output': True,
                                               'output_path': 'census_bureau_API_output'},
             'geocode_service_ARC_GIS': {'use_ArcGIS_API': True,
                                         'output_path': 'ArcGIS_API_output',
                                         'save_differance_list': True,
                                         'differance_list_path': 'diff_list',
                                         'save_output': True,
                                         'output_path': 'ArcGIS_output'},
             'addresses_census_tracked_path': 'addresses_cencus_tratced',
             'merge_to_me_database': {'merge_orig': False, 'Orig_Path': 'meOrig.csv'}}
main(paramters)
