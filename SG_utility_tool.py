################################################################################
# written by Francis Osei Tutu Afrifa, 2024.
################################################################################

### IMPORT NECESSARY PACKAGES ###
import csv
import sys
import re
import os
from zipfile import ZipFile
import gzip
import geopandas as gp
import numpy as np
import pandas as pd
from geopy.distance import geodesic

class Extract:
    def __init__(self):
        maxInt = sys.maxsize
        while True:
            try:
                csv.field_size_limit(maxInt)
                break
            except OverflowError:
                maxInt = int(maxInt / 10)


    def get_naics_mask(self, df, naics_code):
        """
        This function creates a mask to filter NAICS codes of varying lengths in a dataframe.

        naics_code: str
                A string of digits of the North American Industry Classification System (NAICS) codes representing 
                the business to extract POIs. Eg. For elmentary and secondary schools: ['611110'], For education: ['61']
        
        """
        n = len(naics_code)
        naics_mask = [(not pd.isna(ncode)) and (str(int(ncode))[:n] == naics_code) for ncode in list(df['naics_code'])]
        return naics_mask
    
    
    def POIs(self,naics_codes, region_filter, base_filepath, core_poi_filenames, results_filepath):
        """
        This function extracts the Points or places of interest (POIs) based on the specified naics_code(s).

        naics_codes: list
                A string list of digits of the North American Industry Classification System (NAICS) codes representing 
                the business to extract POIs. Eg. For elmentary and secondary schools: ['611110'], For education: ['61']
                 Use 'All' or ['All'] to extract all NAICS CODES

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
        output_file_writers = []
    
        for i in range(len(naics_codes)):
            output_file = open(results_filepath + naics_codes[i] + '.csv', 'w', encoding="utf-8")
            output_file_writer = csv.writer(output_file)
            col_names = ['safegraph_place_id','parent_safegraph_place_id','location_name',
                         'safegraph_brand_ids','brands','top_category','sub_category',
                         'naics_code','latitude','longitude','street_address','city','region',
                         'postal_code','iso_country_code','phone_number','open_hours','category_tags' ]
            output_file_writer.writerow(col_names)
            output_file_list.append(output_file)
            output_file_writers.append(output_file_writer)
    
        poi_id_dict = {}
    
        for core_poi_filename in core_poi_filenames:
            for file_index in range(1, 6):
                try:
                    input_poi_file_path = base_filepath + core_poi_filename + 'core_poi-part' + str(file_index) + '.csv'
                    print("Processing " + input_poi_file_path)
                    df = pd.read_csv(input_poi_file_path, encoding="unicode-escape")
    
                    # Apply region filters
                    if region_filter[0].lower() == 'state-city':
                        df = df[df['region'].isin(region_filter[1].keys()) & df['city'].isin(region_filter[1].keys())]
                    elif region_filter[0] == 'state':
                        df = df[df['region'].isin(region_filter[1].keys())]
    
                    print(f"DataFrame after region filtering: {df.shape[0]} rows")
    
                    try:
                        all = naics_codes.lower()=='all'
                        if all == True:
                            for _, unfiltered_row in df.iterrows():
                                poi_id = unfiltered_row['safegraph_place_id']
                                if poi_id in poi_id_dict.keys():
                                    continue
                                else:
                                    poi_id_dict[poi_id] = 0
                                    output_file_writers[naics_codes.index(naics_codes[0])].writerow(unfiltered_row[col_names])

                        else:
                            for naics_code in naics_codes:
                                naics_mask = self.get_naics_mask(df, naics_code)
                                df_filtered = df[naics_mask]
                                print(f"DataFrame after NAICS filtering ({naics_code}): {df_filtered.shape[0]} rows")
            
                                for _, filtered_row in df_filtered.iterrows():
                                    poi_id = filtered_row['safegraph_place_id']
                                    if poi_id in poi_id_dict.keys():
                                        continue
                                    else:
                                        poi_id_dict[poi_id] = 0
                                        output_file_writers[naics_codes.index(naics_code)].writerow(filtered_row[col_names])

                    except AttributeError:
                        all = naics_codes[0].lower()=='all'
                        if all == True:
                            for _, unfiltered_row in df.iterrows():
                                poi_id = unfiltered_row['safegraph_place_id']
                                if poi_id in poi_id_dict.keys():
                                    continue
                                else:
                                    poi_id_dict[poi_id] = 0
                                    output_file_writers[naics_codes.index(naics_codes[0])].writerow(unfiltered_row[col_names])

                        else:
                            for naics_code in naics_codes:
                                naics_mask = self.get_naics_mask(df, naics_code)
                                df_filtered = df[naics_mask]
                                print(f"DataFrame after NAICS filtering ({naics_code}): {df_filtered.shape[0]} rows")
            
                                for _, filtered_row in df_filtered.iterrows():
                                    poi_id = filtered_row['safegraph_place_id']
                                    if poi_id in poi_id_dict.keys():
                                        continue
                                    else:
                                        poi_id_dict[poi_id] = 0
                                        output_file_writers[naics_codes.index(naics_code)].writerow(filtered_row[col_names])
    
                except FileNotFoundError:
                    print("First Read attempt failed")
                    try:
                        zip_file = ZipFile(base_filepath + core_poi_filename + '.zip')
                        unzipped_file = zip_file.namelist()
                        with zip_file.open(sorted([file for file in unzipped_file if file.endswith('csv.gz')])[file_index - 1], 'r') as csv_gz:
                            df = pd.read_csv(csv_gz, compression='gzip', encoding="unicode-escape")
    
                            # Apply region filters
                            if region_filter[0].lower() == 'state-city':
                                df = df[df['region'].isin(region_filter[1].values()) & df['city'].isin(region_filter[1].keys())]
                            elif region_filter[0] == 'state':
                                df = df[df['region'].isin(region_filter[1].keys())]
    
                            print(f"DataFrame after region filtering: {df.shape[0]} rows")
    
                            try:
                                all = naics_codes.lower()=='all'
                                if all == True:
                                    for _, unfiltered_row in df.iterrows():
                                        poi_id = unfiltered_row['safegraph_place_id']
                                        if poi_id in poi_id_dict.keys():
                                            continue
                                        else:
                                            poi_id_dict[poi_id] = 0
                                            output_file_writers[naics_codes.index(naics_codes[0])].writerow(unfiltered_row[col_names])

                                else:
                                    for naics_code in naics_codes:
                                        naics_mask = self.get_naics_mask(df, naics_code)
                                        df_filtered = df[naics_mask]
                                        print(f"DataFrame after NAICS filtering ({naics_code}): {df_filtered.shape[0]} rows")
            
                                        for _, filtered_row in df_filtered.iterrows():
                                            poi_id = filtered_row['safegraph_place_id']
                                            if poi_id in poi_id_dict.keys():
                                                continue
                                            else:
                                                poi_id_dict[poi_id] = 0
                                                output_file_writers[naics_codes.index(naics_code)].writerow(filtered_row[col_names])

                            except AttributeError:
                                all = naics_codes[0].lower()=='all'
                                if all == True:
                                    for _, unfiltered_row in df.iterrows():
                                        poi_id = unfiltered_row['safegraph_place_id']
                                        if poi_id in poi_id_dict.keys():
                                            continue
                                        else:
                                            poi_id_dict[poi_id] = 0
                                            output_file_writers[naics_codes.index(naics_codes[0])].writerow(unfiltered_row[col_names])

                                else:
                                    for naics_code in naics_codes:
                                        naics_mask = self.get_naics_mask(df, naics_code)
                                        df_filtered = df[naics_mask]
                                        print(f"DataFrame after NAICS filtering ({naics_code}): {df_filtered.shape[0]} rows")
            
                                        for _, filtered_row in df_filtered.iterrows():
                                            poi_id = filtered_row['safegraph_place_id']
                                            if poi_id in poi_id_dict.keys():
                                                continue
                                            else:
                                                poi_id_dict[poi_id] = 0
                                                output_file_writers[naics_codes.index(naics_code)].writerow(filtered_row[col_names])
                    except FileNotFoundError:
                        print("Second Read attempt failed")
                        try:
                            input_poi_file_path = base_filepath + core_poi_filename + 'core_poi-part' + str(file_index) + '.csv.gz'
                            print("Processing " + input_poi_file_path)
                            df = pd.read_csv(input_poi_file_path, compression='gzip', encoding="unicode-escape")
    
                            # Apply region filters
                            if region_filter[0].lower() == 'state-city':
                                df = df[df['region'].isin(region_filter[1].values()) & df['city'].isin(region_filter[1].keys())]
                            elif region_filter[0] == 'state':
                                df = df[df['region'].isin(region_filter[1].keys())]
    
                            print(f"DataFrame after region filtering: {df.shape[0]} rows")

                            try:
                                all = naics_codes.lower()=='all'
                                if all == True:
                                    for _, unfiltered_row in df.iterrows():
                                        poi_id = unfiltered_row['safegraph_place_id']
                                        if poi_id in poi_id_dict.keys():
                                            continue
                                        else:
                                            poi_id_dict[poi_id] = 0
                                            output_file_writers[naics_codes.index(naics_codes[0])].writerow(unfiltered_row[col_names])

                                else:
                                    for naics_code in naics_codes:
                                        naics_mask = self.get_naics_mask(df, naics_code)
                                        df_filtered = df[naics_mask]
                                        print(f"DataFrame after NAICS filtering ({naics_code}): {df_filtered.shape[0]} rows")
            
                                        for _, filtered_row in df_filtered.iterrows():
                                            poi_id = filtered_row['safegraph_place_id']
                                            if poi_id in poi_id_dict.keys():
                                                continue
                                            else:
                                                poi_id_dict[poi_id] = 0
                                                output_file_writers[naics_codes.index(naics_code)].writerow(filtered_row[col_names])

                            except AttributeError:
                                all = naics_codes[0].lower()=='all'
                                if all == True:
                                    for _, unfiltered_row in df.iterrows():
                                        poi_id = unfiltered_row['safegraph_place_id']
                                        if poi_id in poi_id_dict.keys():
                                            continue
                                        else:
                                            poi_id_dict[poi_id] = 0
                                            output_file_writers[naics_codes.index(naics_codes[0])].writerow(unfiltered_row[col_names])

                                else:
                                    for naics_code in naics_codes:
                                        naics_mask = self.get_naics_mask(df, naics_code)
                                        df_filtered = df[naics_mask]
                                        print(f"DataFrame after NAICS filtering ({naics_code}): {df_filtered.shape[0]} rows")
            
                                        for _, filtered_row in df_filtered.iterrows():
                                            poi_id = filtered_row['safegraph_place_id']
                                            if poi_id in poi_id_dict.keys():
                                                continue
                                            else:
                                                poi_id_dict[poi_id] = 0
                                                output_file_writers[naics_codes.index(naics_code)].writerow(filtered_row[col_names])
                        except FileNotFoundError:
                            print("Oh God")
                            raise FileNotFoundError("All attempts failed.")
    
        print('Relevant POIs Successfully extracted!')
        for output_file in output_file_list:
            output_file.close()
        #print(open(results_filepath + naics_codes[i] + '.csv').read())
        return output_file_list

    def add_dicts(self, dict1, dict2):
        result_dict = {}
        for key in dict1:
            if key in dict2:
                result_dict[key] = dict1[key] + dict2[key]
            else:
                result_dict[key] = dict1[key]
        for key in dict2:
            if key not in result_dict:
                result_dict[key] = dict2[key]
        return result_dict

    def county_weekly_visits(self, base_filepath, POI_filepath, weekly_pattern_filepaths, path_to_shapefile_CBG,
                                 path_to_shapefile_CT, path_to_save_weekly_patterns, path_to_save_weekly_flow_format,
                                 naics_codes, naics_names, date_range_end, county_FIP, df_weekly_visits_avail=True,
                                extract_based_on='Census Tracts', kepler_format='Yes', demo_data = True):
        """
        This function extracts weekly flows (visit and or visitor counts) based on the specified naics_code(s) for a given county.
    
        base_filepath: path
                    A path of type string to your base directory containing all the weekly places patterns data
    
        POI_filepath: path
                    A path of type string to your directory containing selected extracted POI csv file
    
        weekly_pattern_filepaths: list
                    Define a list of paths to select weekly places pattern files from all the weekly places patterns data in your base_filepath (i.e. subset
                    of files from your weekly places patterns data)
    
        path_to_save_weekly_patterns: str
                    Define a directory to store the results for the extracted weekly places patterns.
    
        path_to_shapefile_CBG: str (required)
                    Define the path to the Census Blocks Shapefile for the state or region of interest.
    
        path_to_shapefile_CT: str
                    Define the path to the Census Tracts Shapefile for the state or region of interest.
    
        path_to_save_kepler_format: path
                    Define a directory to store the results in a kepler file format ([target_LON,target_LAT,residence_LON, flows/visitor_count,residence_LAT])
                    for the extracted weekly places patterns.
    
        naics_codes: list
                    A string list of 6 or 4-digit North American Industry Classification System (NAICS) codes representing 
                    the business to extract POIs. Eg. For schools: ['611110']. This is the default value.
    
        naics_names: list
                    A string list of names of the NAICS codes representing the business to extract POIs. Default is ['School'].
    
        county_FIP: str
                    A 5-digit string code representing state and county for selection. First two digits represent the state code 
                    while the last three digits represent the county code. Defaults to '48201' representing Texas-Harris county
    
        df_weekly_visits_avail: Boolean
                    Specify True or False to specify whether dataframe of county weekly raw visits and visitor flows is already available or not. 
                    If False, first extract the weekly county raw visits and visitor counts and stores it as a csv file first.
                    If True, skip this process.
    
        date_range_end: str
                    End date for selection of the week interested in. Eg. for week 1: '2021-01-04'
    
        extract_based_on: str
                    Specify whether to extract weekly visits and provide outputs based on 'CBGs', 'Census Tracts' or 'both'. Defaults to 'both'
    
        kepler_format: str
                    Specify whether 'Yes' or 'No'. 'Yes' provides output results in a kepler format. Defaults to 'Yes'
        
        Returns csv files of extracted weekly visitor flow patterns based on specified naics_codes and stores them in path_to_save_weekly_flow_format directory.
        
        """
        if (df_weekly_visits_avail==False) & (demo_data==False):
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
                                            home_cbg_week_visitors_dict[constructed_output_id] = self.add_dicts(home_cbg_week_visitors_dict[constructed_output_id],extracted_dict)
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
    
    
        elif (df_weekly_visits_avail==False) & (demo_data==True):
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
                                        poi_cbg = row[15]#[0:5]
                                        date_range_start = row[9][0:10]
                                        date_range_end = row[10][0:10]
                                        naics_code = naics_codes[i]
                                        location_name = row[1]
        
                                        visits = int(row[11])
                                        visitors = int(row[12])
                                        extracted_dict = eval(re.search(r"\{(.*?)\}", row[16]).group(0))
        
                                        constructed_output_id = date_range_start+"|"+date_range_end+"|"+poi_cbg+"|"+location_name+"|"+naics_code
        
                                        if constructed_output_id in cbg_week_visits_dict.keys():
                                            cbg_week_visits_dict[constructed_output_id] =  cbg_week_visits_dict[constructed_output_id] + visits
                                            cbg_week_visitors_dict[constructed_output_id] = cbg_week_visitors_dict[constructed_output_id] + visitors
                                            home_cbg_week_visitors_dict[constructed_output_id] = self.add_dicts(home_cbg_week_visitors_dict[constructed_output_id],extracted_dict)
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
                cbg_week_files = sorted(os.listdir(base_filepath+'home_panel_summary/'))
        
                for cbg_week_samplesize_file in cbg_week_files:
                    if cbg_week_samplesize_file.endswith(".csv"):
                        input_panel_size_file_path = base_filepath+'home_panel_summary/'+cbg_week_samplesize_file
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
    
        
        col_names = ['safegraph_place_id','parent_safegraph_place_id','location_name',
                     'safegraph_brand_ids','brands','top_category','sub_category',
                     'naics_code','latitude','longitude','street_address','city','region',
                     'postal_code','iso_country_code','phone_number','open_hours','category_tags' ]
        POIs_df = pd.read_csv(POI_filepath+naics_codes[0]+'.csv',names=col_names)
        POIs_df = POIs_df.drop(0)
        POIs_df = POIs_df.set_index(POIs_df['safegraph_place_id'])
        del POIs_df['safegraph_place_id']
        POIs_df = POIs_df.reset_index()
    
    
        df_county_visits = pd.read_csv(path_to_save_weekly_patterns+'.csv')
        df_county_visits = df_county_visits.sort_values(by=['Census_Block_Groups', 'date_range_start', 'date_range_end'])
        
        ##### Merge weekly_visit and POI dataframes based on the 'location name' column
        # Placeholder for the new columns
        df_county_visits['longitude'] = None
        df_county_visits['latitude'] = None
        
        # Iterate over the rows of the df_county_visits dataframe
        for index, row in df_county_visits.iterrows():
            # Find the matching row in the main dataframe
            matching_row = POIs_df[(POIs_df['location_name'] == row['location_name'])]
        
            # If there is a match, update the coordinates in df_hc_visits
            if not matching_row.empty:
                df_county_visits.at[index, 'longitude'] = matching_row.iloc[0]['longitude']
                df_county_visits.at[index, 'latitude'] = matching_row.iloc[0]['latitude']
        
        # df_county_visits now contains the additional columns from POIs_df where the longitude and latitude values match
        df_county_visits = df_county_visits.reset_index()
        del df_county_visits['index']
    
        _state_cb = gp.read_file(path_to_shapefile_CBG)
        _state_cb = _state_cb[['STATEFP', 'COUNTYFP', 'TRACTCE', 'BLKGRPCE', 'GEOID', 'geometry']]
        _state_cb['LON'] = _state_cb['geometry'].centroid.x
        _state_cb['LAT'] = _state_cb['geometry'].centroid.y
        _county_CBG = _state_cb[_state_cb.COUNTYFP.values==county_FIP[2:]]
    
        week_ = df_county_visits[df_county_visits['date_range_end'] == date_range_end]
        week_home_cbg_list = []
        for i in np.arange(0,len(week_['visitor_home_cbgs'])):
            week_home_cbg_list.append(eval(re.search(r"\{(.*?)\}", week_['visitor_home_cbgs'].reset_index(drop=True)[i]).group(0)))
        print('Done')
        week_temp = pd.DataFrame({naics_names[0]+'_CBG': week_['Census_Block_Groups'].tolist(), 'home_cbg_dict': week_home_cbg_list,
                                  naics_names[0]+'_LON': week_['longitude'].tolist(), naics_names[0]+'_LAT': week_['latitude'].tolist(),
                                  'date_range_start':week_['date_range_start'].tolist(), 'date_range_end':week_['date_range_end'].tolist()
                                 })
        print('Next')
        
        week_new_df = (pd.DataFrame(week_temp['home_cbg_dict'].tolist(), index=week_temp[[naics_names[0]+'_CBG',
                                                                                          naics_names[0]+'_LON', 
                                                                                          naics_names[0]+'_LAT',
                                                                                          'date_range_start',
                                                                                          'date_range_end']])
                    .reset_index()
                    .melt('index', var_name='GEOID', value_name='Visitor_Count')
                    .dropna()
                    .reset_index(drop=True))
        
        week_new_df_final = pd.DataFrame(week_new_df['index'].tolist(), columns=[naics_names[0]+'_CBG',naics_names[0]+'_LON',
                                                                                 naics_names[0]+'_LAT','date_range_start',
                                                                                          'date_range_end'])
        week_new_df_final['GEOID'] = week_new_df ['GEOID']
        week_new_df_final['Visitor_Count'] = week_new_df ['Visitor_Count']
        
        #### select visitor/home CBGs in specified county only
        county_home_CBGs = week_new_df_final[week_new_df_final['GEOID'].str.match(county_FIP)]
    
    
        #### Based on Census Tracts:
        _state_ct = gp.read_file(path_to_shapefile_CT)
    
        _state_ct = _state_ct[['STATEFP', 'COUNTYFP', 'TRACTCE', 'GEOID', 'geometry']]
        _state_ct['LON'] = _state_ct['geometry'].centroid.x
        _state_ct['LAT'] = _state_ct['geometry'].centroid.y
        _county_CT = _state_ct[_state_ct.COUNTYFP.values==county_FIP[2:]]
    
        county_home_CBGs[naics_names[0]+'_CBG'] = county_home_CBGs[naics_names[0]+'_CBG'].values.astype(str)
        
        county_CTs = county_home_CBGs.groupby([county_home_CBGs[naics_names[0]+'_CBG'].str[5:11],'date_range_start',
               'date_range_end', county_home_CBGs['GEOID'].str[5:11]])['Visitor_Count'].sum().reset_index(name='Tot. Visitor_count')
        home_county_CTs = county_CTs.copy()
        home_county_CTs.columns = [naics_names[0]+'_TRACTCE','date_range_start','date_range_end','TRACTCE', 'Visitor_Count']
    
        #### Merge 
        merged_on_home_CTS = home_county_CTs.copy()
        # Placeholder for the new columns
        merged_on_home_CTS['STATEFP'] = None
        merged_on_home_CTS['COUNTYFP'] = None
        merged_on_home_CTS['Home_LON'] = None
        merged_on_home_CTS['Home_LAT'] = None
        
        # Iterate over the rows of the merged_on_home_CTS dataframe
        for index, row in merged_on_home_CTS.iterrows():
            # Find the matching row in the main dataframe
            matching_row = _county_CT[(_county_CT['TRACTCE'] == row['TRACTCE'])]
        
            # If there is a match, update the coordinates in merged_on_home_CTS
            if not matching_row.empty:
                merged_on_home_CTS.at[index, 'Home_LON'] = matching_row.iloc[0]['LON']
                merged_on_home_CTS.at[index, 'Home_LAT'] = matching_row.iloc[0]['LAT']
                merged_on_home_CTS.at[index, 'STATEFP'] = matching_row.iloc[0]['STATEFP']
                merged_on_home_CTS.at[index, 'COUNTYFP'] = matching_row.iloc[0]['COUNTYFP']
        
        # merged_on_home_CTS now contains the additional columns from _county_CT where the TRACTCE values match
        
        merged_on_home_CTS.columns = ['TRACTCE', 'date_range_start', 'date_range_end', 'Home_TRACTCE',
                                      'Visitor_count', 'STATEFP', 'COUNTYFP', 'Home_LON', 'Home_LAT']
        
        merged_on_NAICS_CTS = merged_on_home_CTS.copy()
        # Placeholder for the new columns
        merged_on_NAICS_CTS[naics_names[0]+'_LON'] = None
        merged_on_NAICS_CTS[naics_names[0]+'_LAT'] = None
        
        # Iterate over the rows of the merged_on_NAICS_CTS dataframe
        for index, row in merged_on_NAICS_CTS.iterrows():
            # Find the matching row in the main dataframe
            matching_row = _county_CT[(_county_CT['TRACTCE'] == row['TRACTCE'])]
        
            # If there is a match, update the coordinates in merged_on_NAICS_CTS
            if not matching_row.empty:
                merged_on_NAICS_CTS.at[index, naics_names[0]+'_LON'] = matching_row.iloc[0]['LON']
                merged_on_NAICS_CTS.at[index, naics_names[0]+'_LAT'] = matching_row.iloc[0]['LAT']
                
        # merged_on_NAICS_CTS now contains the additional columns from _county_CT where the GEOID values match
        
        merged_on_NAICS_CTS.columns = [naics_names[0]+'_TRACTCE', 'date_range_start', 'date_range_end', 'Home_TRACTCE',
                                      'Visitor_Count', 'STATEFP', 'COUNTYFP', 'Home_LON', 'Home_LAT',naics_names[0]+'_LON',
                                       naics_names[0]+'_LAT']
        
        merged_on_NAICS_CTS = merged_on_NAICS_CTS[['date_range_start','date_range_end',naics_names[0]+'_TRACTCE','Home_TRACTCE','STATEFP',
                                                'COUNTYFP','Visitor_Count','Home_LON','Home_LAT',naics_names[0]+'_LON',naics_names[0]+'_LAT']]
        merged_on_NAICS_CTS = merged_on_NAICS_CTS.dropna().reset_index()
        del merged_on_NAICS_CTS['index']
    
        ####offset naics_CBGS lon and lat:
        merged_on_NAICS_CTS[naics_names[0]+'_LON'] = merged_on_NAICS_CTS[naics_names[0]+'_LON']+0.02
        merged_on_NAICS_CTS[naics_names[0]+'_LAT'] = merged_on_NAICS_CTS[naics_names[0]+'_LAT']+0.02
        
        dist_array = []
        for i in range(0,len(merged_on_NAICS_CTS)):
            dist = geodesic(merged_on_NAICS_CTS[['Home_LAT','Home_LON']].values[i],
                            merged_on_NAICS_CTS[[naics_names[0]+'_LAT',naics_names[0]+'_LON']].values[i]).km
            dist_array.append(dist)
    
        merged_on_NAICS_CTS['Distance_Covered (km)']=dist_array
        
        if kepler_format == 'Yes':
            week_kepler_CT = merged_on_NAICS_CTS[[naics_names[0]+'_LON',naics_names[0]+'_LAT','Home_LON', 'Visitor_Count', 'Home_LAT']]
            week_kepler_CT.to_csv(path_to_save_weekly_flow_format+'-kepler-CT.csv',encoding="UTF-8", index=False)
    
        else:
            merged_on_NAICS_CTS.to_csv(path_to_save_weekly_flow_format+'-CT.csv',encoding="UTF-8", index=False)
            network_analysis_CT = merged_on_NAICS_CTS[[naics_names[0]+'_TRACTCE', 'Home_TRACTCE', 'Visitor_Count',
                                                       naics_names[0]+'_LON',naics_names[0]+'_LAT','Home_LON', 'Home_LAT',
                                                       'Distance_Covered (km)']]
            network_analysis_CT.to_csv(path_to_save_weekly_flow_format+'-network_analysis-CT.csv',encoding="UTF-8", index=False)



    def POI_Census_Tract_count(self, poi_df, path_to_shapefile_CT, naics_name):
        """
        This function returns a two column dataframe of Census Tract and number of POIs in each Tract

        poi_df: pandas dataframe
               The pandas dataframe containing the extracted POIs of interest
               
        path_to_shapefile_CT: str
                Define the path to the Census Tracts Shapefile for the state or region of interest.

        naics_name: str
                A string of name of the NAICS code representing the business to extract the number of counts for each census tract. Eg: 'EducatioN POIs'.
    
        """
        from shapely.geometry import Point
        # Using the POIs dataframe 'longitude' and 'latitude' columns
        # Convert POIs to a GeoDataFrame
        geometry = [Point(xy) for xy in zip(poi_df['longitude'].astype(float), poi_df['latitude'].astype(float))]
        gdf_pois = gp.GeoDataFrame(poi_df, geometry=geometry)
    
        # Load the census tracts shapefile (this should be replaced with the actual path to the shapefile)
        census_tracts = gp.read_file(path_to_shapefile_CT)
    
        #use the same coordinate reference system (CRS) for both GeoDataFrames
        gdf_pois = gdf_pois.set_crs(census_tracts.crs, allow_override=True)
    
        # Perform the spatial join
        joined_gdf = gp.sjoin(gdf_pois, census_tracts, how='left', op='within')
    
        # Count the number of POIs in each census tract
        tract_poi_counts = joined_gdf.groupby('TRACTCE').size().reset_index(name='Number of '+naics_name)
        return tract_poi_counts


# Example usage:
# extractor = Extract()
# extractor.POIs([...])
# extractor.county_weekly_visits([...])
