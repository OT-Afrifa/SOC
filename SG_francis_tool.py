                        ################################################################################
                                    #written by Francis Osei Tutu Afrifa, 2024.
        
        
                                            ### IMPORT NECESSARY PACKAGES ###
                        ################################################################################
import csv
import sys
import re
import os
from zipfile import ZipFile
import gzip

maxInt = sys.maxsize

while True:
    # decrease the maxInt value by factor 10 
    # as long as the OverflowError occurs.

    try:
        csv.field_size_limit(maxInt)
        break
    except OverflowError:
        maxInt = int(maxInt/10)
        

#state_code_dict = {'CO':'08','FL':'12','LA':'22','TX':'48','WI':'55'}
def extract_POIs(naics_codes,region_filter,base_filepath,core_poi_filenames,results_filepath):

    """
    This function extracts the Points or places of interest (POIs) based on the specified naics_code(s).

    naics_codes: list
                A string list of 6 or 4-digit North American Industry Classification System (NAICS) codes representing 
                the business to extract POIs. Eg. For schools: ['611110']

    region_filter: list
                Specify a POI filter scale of the format: [scale_type, filter_items] to select regions by an entire state or a city in the state,
                where scale_type can be 'state' or 'state-city' and the filter items is a dictionary containing the city or state name and 
                corresponding 2-digit ID of type string. Eg. ['state', {'TX':'48'}] or ['state-city', {'Houston': '48'}]

    base_filepath: path
                A path of type string to your base directory containing all the core POIs
                
    core_poi_filenames: list
                Define a list of paths to select core_poi files from all the core POIs in your base_filepath (i.e. subset
                of files from your core POIs)

    results_filepath: path
                Define a directory to store the results

    Returns csv files of extracted POIs based on specified naics_codes and region_filter and saved in results_filepath directory.
    """
       
    output_file_list = []
    
    for i in range(len(naics_codes)):
        output_file =  open(results_filepath+naics_codes[i]+'.csv', 'w', encoding="utf-8")
        output_file_writer = csv.writer(output_file)
     
    output_file_writer.writerow(['safegraph_place_id','parent_safegraph_place_id','location_name','safegraph_brand_ids','brands','top_category','sub_category',
                                'naics_code','latitude','longitude','street_address','city','region','postal_code','iso_country_code','phone_number','open_hours','category_tags' ]
                                )
    output_file_list.append(output_file_writer) 
    
    poi_id_dict = {}
    for core_poi_filename in core_poi_filenames:
        for file_index in range(1,6):
            try:
                input_poi_file_path = base_filepath+core_poi_filename+'core_poi-part' + str(file_index)+'.csv'
                print("Processing "+input_poi_file_path)
                
                with open(input_poi_file_path, 'r', encoding="unicode-escape") as csv_file:
                    file_reader = csv.reader(csv_file, delimiter=',', quotechar='"')
                    first_row= True
                    for row in file_reader:
                        if first_row:
                            first_row = False
                            continue   

                        country_code = row[14]
                        if country_code != 'US':
                            continue


                        if region_filter[0].lower()=='state-city':
                            region_code = row[12]
                            city_code = row[11]
                            if (region_code not in region_filter[1].keys()) or city_code not in region_filter[1].keys():
                                continue
                                
                        elif region_filter[0]=='state':
                            region_code = row[12]
                            if region_code not in region_filter[1].keys():
                                continue
                        

                        naics_code = row[7]
                        if naics_code not in naics_codes:
                            continue


                        poi_id = row[0]
                        if poi_id in poi_id_dict.keys():
                            continue
                        else:
                            poi_id_dict[poi_id] = 0 #+"|"+sub_cate
                            #print(row)

                            for i in range(len(naics_codes)):
                                if naics_code == naics_codes[i]:
                                    output_file_list[i].writerow(row)
            except FileNotFoundError:
                print("First Read attempt failed")
                try:
                    zip_file = ZipFile(base_filepath+core_poi_filename+'.zip')
                    unzipped_file = zip_file.namelist()
                    #input_poi_file_path = base_filepath+core_poi_filename+'/core_poi-part' + str(file_index)+'.csv'
                    #print("Processing "+input_poi_file_path)

                    with zip_file.open(sorted([file for file in unzipped_file if file.endswith('csv.gz')])[file_index-1] , 'r') as csv_gz:
                        with gzip.open(csv_gz, 'rt') as csv_file:
                            file_reader = csv.reader(csv_file, delimiter=',', quotechar='"')
                            first_row= True
                            for row in file_reader:
                                if first_row:
                                    first_row = False
                                    continue   

                                country_code = row[14]
                                if country_code != 'US':
                                    continue


                                if region_filter[0].lower()=='state-city':
                                    region_code = row[12]
                                    city_code = row[11]
                                    if (region_code not in region_filter[1].keys()) or city_code not in region_filter[1].keys():
                                        continue
                                        
                                elif region_filter[0]=='state':
                                    region_code = row[12]
                                    if region_code not in region_filter[1].keys():
                                        continue
                                
                                naics_code = row[7]
                                if naics_code not in naics_codes:
                                    continue


                                poi_id = row[0]
                                if poi_id in poi_id_dict.keys():
                                    continue
                                else:
                                    poi_id_dict[poi_id] = 0 #+"|"+sub_cate
                                    #print(row)

                                    for i in range(len(naics_codes)):
                                        if naics_code == naics_codes[i]:
                                            output_file_list[i].writerow(row)
                except FileNotFoundError:
                    print("Second Read attempt failed")
                    try:
                        input_poi_file_path = base_filepath+core_poi_filename+'core_poi-part' + str(file_index)+'.csv.gz'
                        print("Processing "+input_poi_file_path)

                        with gzip.open(input_poi_file_path, 'rt') as csv_file:
                            file_reader = csv.reader(csv_file, delimiter=',', quotechar='"')
                            first_row= True
                            for row in file_reader:
                                if first_row:
                                    first_row = False
                                    continue

                                country_code = row[14]
                                if country_code != 'US':
                                    continue


                                if region_filter[0].lower()=='state-city':
                                    region_code = row[12]
                                    city_code = row[11]
                                    if (region_code not in region_filter[1].keys()) or city_code not in region_filter[1].keys():
                                        continue
                                        
                                elif region_filter[0]=='state':
                                    region_code = row[12]
                                    if region_code not in region_filter[1].keys():
                                        continue
                                
                                naics_code = row[7]
                                if naics_code not in naics_codes:
                                    continue


                                poi_id = row[0]
                                if poi_id in poi_id_dict.keys():
                                        continue
                                else:
                                    poi_id_dict[poi_id] = 0 #+"|"+sub_cate
                                    #print(row)

                                    for i in range(len(naics_codes)):
                                        if naics_code == naics_codes[i]:
                                            output_file_list[i].writerow(row)

                    except FileNotFoundError:
                        print("Oh God")
                        raise FileNotFoundError("All attempts failed.")
                
    print('Relevant POIs Successfully extracted!')
    output_file.close()
    print(open(results_filepath+naics_codes[i]+'.csv').read())
    return output_file_list
   

def add_dicts(dict1, dict2):
    result_dict = {}

    # Iterate through the keys of the first dictionary
    for key in dict1:
        # If the key is present in both dictionaries, add their values
        if key in dict2:
            result_dict[key] = dict1[key] + dict2[key]
        else:
            # If the key is only in the first dictionary, add it to the result
            result_dict[key] = dict1[key]

    # Iterate through the keys of the second dictionary
    for key in dict2:
        # If the key is only in the second dictionary, add it to the result
        if key not in result_dict:
            result_dict[key] = dict2[key]

    return result_dict


def extract_county_weekly_visits(naics_codes, base_filepath, POI_filepath, weekly_pattern_filepaths,county_FIP, path_to_save_weekly_patterns):
    """
    """
    # step 1: get the ids of specified POI outlets
    movement_outlet_list = []
    lat_list = []
    lon_list = []
    for i in range(len(naics_codes)):
        _store_id_dict = {}
        _lat_id_dict = {}
        _lon_id_dict = {}
        with open(POI_filepath+naics_codes[i]+'.csv', 'r') as csv_file:
            file_reader = csv.reader(csv_file, delimiter=',', quotechar='"')
            first_row= True
            for row in file_reader:
                if first_row:
                    first_row = False
                    continue   

                _store_id = row[0]
                _store_id_dict[_store_id] = 0
                _lat_id = row[8] 
                _lat_id_dict[_lat_id] = 8
                _lon_id = row[9]
                _lon_id_dict[_lat_id] = 9

        movement_outlet_list.append(_store_id_dict)
        lat_list.append(_lat_id_dict)
        lon_list.append(_lon_id_dict)
        
    # step 2: go through the weekly patterns file
    cbg_week_visits_dict = {}
    cbg_week_visitors_dict = {}
    home_cbg_week_visitors_dict = {}
    for path in weekly_pattern_filepaths:
        weekly_pattern_files = sorted(os.listdir(base_filepath+'patterns/'+path))
        for this_pattern_file in weekly_pattern_files:
            if this_pattern_file.endswith(".csv.gz"):
                #print(path,this_pattern_file)
                input_pattern_file_path = base_filepath+'patterns/'+path+this_pattern_file
                print("Processing "+input_pattern_file_path)

                with gzip.open(input_pattern_file_path, 'rt') as csv_file:
                    file_reader = csv.reader(csv_file, delimiter=',', quotechar='"')
                    first_row= True
                    for row in file_reader:
                        #print(row)
                        if first_row:
                            first_row = False
                            continue

                        # if this POI is not a specified movement NAICS code in the specified region, continue
                        poi_id = row[0]
                        for i in range(len(naics_codes)):
                            if poi_id in movement_outlet_list[i].keys():
                                #print(poi_id)
                                poi_cbg = row[18]#[0:5]
                                date_range_start = row[12][0:10]
                                date_range_end = row[13][0:10]
                                naics_code = naics_codes[i]
                                location_name = row[4]

                                visits = int(row[14])
                                visitors = int(row[15])
                                extracted_dict = eval(re.search(r"\{(.*?)\}", row[19]).group(0))

                                constructed_output_id = date_range_start+"|"+date_range_end+"|"+poi_cbg+"|"+location_name+"|"+naics_code

                                if constructed_output_id in cbg_week_visits_dict.keys():
                                    cbg_week_visits_dict[constructed_output_id] =  cbg_week_visits_dict[constructed_output_id] + visits
                                    cbg_week_visitors_dict[constructed_output_id] = cbg_week_visitors_dict[constructed_output_id] + visitors
                                    home_cbg_week_visitors_dict[constructed_output_id] = add_dicts(home_cbg_week_visitors_dict[constructed_output_id],extracted_dict)
                                else:
                                    cbg_week_visits_dict[constructed_output_id] =   visits
                                    cbg_week_visitors_dict[constructed_output_id] =  visitors
                                    home_cbg_week_visitors_dict[constructed_output_id] = extracted_dict

    print()
    print()
    print("DONE PROCESSING WEEKLY PATTERN FILES!!")
    print()
    print()
    
    ### Apply filter to select only specified county
    dictfilt = lambda x, y: dict([ (i,x[i]) for i in x if i in set(y) ])
    filter_opt = [year for year in list(cbg_week_visits_dict.keys()) if ('|'+county_FIP in year)]
    cbg_week_visits_dict_filtered = dictfilt(cbg_week_visits_dict, filter_opt)
    cbg_week_visitors_dict_filtered = dictfilt(cbg_week_visitors_dict, filter_opt)
    home_cbg_week_visitors_dict_filtered = dictfilt(home_cbg_week_visitors_dict, filter_opt)

    # step 3: go through the sample size file
    cbg_week_samplesize_dict = {}

    for path in weekly_pattern_filepaths:
        cbg_week_files = sorted(os.listdir(base_filepath+'home_panel_summary/'+path))

        for cbg_week_samplesize_file in cbg_week_files:
            if cbg_week_samplesize_file.endswith(".csv"):
                input_panel_size_file_path = base_filepath+'home_panel_summary/'+path+cbg_week_samplesize_file
                print("Processing "+input_panel_size_file_path)

                with open(input_panel_size_file_path, 'r') as csv_file:
                    file_reader = csv.reader(csv_file, delimiter=',', quotechar='"')

                    first_row= True
                    for row in file_reader:
                        if first_row:
                            first_row = False
                            continue   

                        cbgs = row[3]#[0:5]
                        date_range_start = row[0][0:10]
                        date_range_end = row[1][0:10]

                        constructed_output_id = date_range_start+"|"+date_range_end+"|"+cbgs

                        sample_size = int(row[4])

                        if constructed_output_id in cbg_week_samplesize_dict.keys():
                            cbg_week_samplesize_dict[constructed_output_id] =  cbg_week_samplesize_dict[constructed_output_id] + sample_size
                        else:
                            cbg_week_samplesize_dict[constructed_output_id] =   sample_size 

    print()
    print()
    print("DONE PROCESSING HOME PANEL CSV FILES!!")
    print()
    print()
    
    # step 4: output the result to a file
    filter_opt = [year for year in list(cbg_week_samplesize_dict.keys()) if ('|'+county_FIP in year)]
    cbg_week_samplesize_dict_filtered = dictfilt(cbg_week_samplesize_dict, filter_opt)
    output_file =  open(path_to_save_weekly_patterns+'.csv', 'w')
    output_file_writer = csv.writer(output_file)
    output_file_writer.writerow(["Census_Block_Groups","date_range_start","date_range_end","raw_visit_counts",
                                 "raw_visitor_counts","visitor_home_cbgs","location_name","NAICS","samplesize"])
        #output_file_list.append(output_file_writer)


    for constructed_id in cbg_week_visits_dict_filtered.keys():
        constructed_id_info = constructed_id.split("|")

        date_range_start = constructed_id_info[0]
        date_range_end = constructed_id_info[1]
        cbgs = constructed_id_info[2]
        location_name = constructed_id_info[3]
        naics_code = constructed_id_info[4]

        visits = cbg_week_visits_dict_filtered[constructed_id]
        visitors = cbg_week_visitors_dict_filtered[constructed_id]
        visitor_home_cbgs = home_cbg_week_visitors_dict_filtered[constructed_id]

        samplesize = cbg_week_samplesize_dict_filtered[date_range_start+"|"+date_range_end+"|"+cbgs]

        #for i in range(len(naics_code_range)):
        #    if naics_code == naics_code_range[i]:
        output_file_writer.writerow([cbgs,date_range_start,date_range_end,visits,visitors,visitor_home_cbgs,
                                     location_name,naics_code,samplesize])


    print(open(path_to_save_weekly_patterns+'.csv').read())
    output_file.close()
    print()
    print()
    print(f'Successfully extracted the weekly visits for {naics_codes[0]}!')
    #output_file.close()
