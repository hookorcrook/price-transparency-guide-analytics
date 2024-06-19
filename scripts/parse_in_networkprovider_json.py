import pandas as pd
import json
from pandas import DataFrame as df
import os
import time
import re

# This script parses json for in-network provider rates data and generates csv files 

# Set to 1 to generate combined file
GENERATE_COMBINED_FILE = 0

start_time = time.time()

# Provide folder path to generate output files
outputdirectory = 'outputfiles'

# Traverse directory to process files
filepath = os.path.join(os.getcwd(),'files')
dir_list = os.listdir(filepath)

combined_df_list = []
counter = 1

# Extract Data for each file in the directory
for file in dir_list:
    file_start_time = time.time()
    state_name = re.search('UnitedHealthcare-of-(.*)--', file).group(1)

    fullfilepath = os.path.join(filepath,file)
    with open(fullfilepath) as f:
        dat = f.read()
        json_data = json.loads(dat)

    try: 

# Extracting provider data
        print(f"\nTask Started: Parsing Provider Reference Data For File : {file} \n")

        provider_data = []
        reporting_entity_name = json_data['reporting_entity_name']
        reporting_entity_type = json_data['reporting_entity_type']
        last_updated_on = json_data['last_updated_on']
        version = json_data['version']
        for x in range (0,len(json_data['provider_references'])-1):
            for provider_group in json_data['provider_references'][x]['provider_groups']:
                for npi in provider_group['npi']:
                    provider_info = {
                        'reporting_entity_name': reporting_entity_name,
                        'reporting_entity_type': reporting_entity_type,
                        'last_updated_on': last_updated_on,
                        'version': version,
                        'npi': npi,
                        'tin_type': provider_group['tin']['type'],
                        'tin_value': provider_group['tin']['value'],
                        'provider_group_id' : json_data['provider_references'][x]['provider_group_id']
                    }
                    provider_data.append(provider_info)

        print(f"\nTask Completed : Parsing Provider Reference Data For File : {file}\n")
        # Creating DataFrame to store provider data
        df1 = pd.DataFrame(provider_data)

        # Extracting in-network rates data
        print(f"\nTask Started : Parsing In-Network Rates Data For File : {file}\n")

        in_network_data = []
        for y in range (0,len(json_data['in_network'])-1):
            for negotiated_rates in json_data['in_network'][y]['negotiated_rates']:
                for neg_data in negotiated_rates['negotiated_prices']:
                    for provider_reference in negotiated_rates['provider_references']:
                        in_network_dict = {
                            'negotiation_arrangement':json_data['in_network'][y]['negotiation_arrangement'],
                            'name':json_data['in_network'][y]['name'].strip(),
                            'billing_code_type':json_data['in_network'][y]['billing_code_type'],
                            'billing_code_type_version': json_data['in_network'][y]['billing_code_type_version'],
                            'billing_code': json_data['in_network'][y]['billing_code'],
                            'description':json_data['in_network'][y]['description'].strip(),
                            'provider_references': provider_reference,
                            'negotiated_prices': neg_data['negotiated_rate'],
                            'service_code': ','.join(map(str, neg_data['service_code'])),       
                            'negotiated_type': neg_data['negotiated_type'],
                            'expiration_date': neg_data['expiration_date'],
                            'billing_class': neg_data['billing_class'],
                            'billing_code_modifier': ','.join(map(str, neg_data['billing_code_modifier'])),
                            'additional_information': neg_data['additional_information'].strip()
                        }
                        in_network_data.append(in_network_dict)

    
        print(f"\nTask Completed : Parsing In-Network Rates Data For File : {file}\n")
        # Creating DataFrame to store provider data   
        df2 = pd.DataFrame(in_network_data)


        df = pd.merge(df1[['reporting_entity_name','reporting_entity_type','npi','provider_group_id']]
                      ,df2[['name','billing_code_type','billing_code','billing_code_type_version',
                            'negotiation_arrangement','negotiated_prices','negotiated_type',
                            'expiration_date','service_code','billing_class','provider_references']]
                      ,how='inner'
                      ,left_on = 'provider_group_id'
                      , right_on= 'provider_references'
                      ).drop(columns='provider_references')

        #df = df.drop(columns='provider_references').drop_duplicates()
        # Output Files
        print(f"\nTask Started : Writing to CSV files For File : {file}\n")

        #df1.to_csv(outputdirectory + '\\providerreferences_' + state_name + '_file_' + str(counter) + '.csv')
        #df2.to_csv(outputdirectory + '\\innetworkrates_' + '_file_' + str(counter) + '.csv')

        df.to_csv(outputdirectory + '\\provider_in-networkrates_' + state_name + '_file_' + str(counter) + '.csv')

        print(f"\nTask Completed : Writing to CSV files For File : {file}\n")
        if(GENERATE_COMBINED_FILE == 1):
            combined_df_list.append(df) 
        counter+=1

    except Exception as e: 
        print(e)
        raise

    finally:
        f.close()
        file_end_time = time.time()
        elapsed_time = file_end_time - file_start_time
        print(f"\n--- Parsed File {file} in Time: {elapsed_time:.2f} seconds ---")

if(GENERATE_COMBINED_FILE == 1):
    print(f"\nTask Started : Writing to Combined CSV file For {counter-1} files\n")
    combined_df = pd.concat(combined_df_list)
    combined_df.to_csv(outputdirectory + '\\provider_in-networkrates_combinedfile.csv')
    print(f"\nTask Completed : Writing to Combined CSV files For {counter-1} files\n")

end_time = time.time() 
total_elapsed_time = end_time-start_time
print(f"\n--- Total Execution Time: {total_elapsed_time:.2f} seconds ---\n")
    