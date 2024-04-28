import pandas as pd
import json
from pandas import DataFrame as df
import os
import time
import re

# This script parses json for out-of-network provider rates data and generates csv files 


start_time = time.time()

# Provide folder path to generate output files
outputdirectory = 'outputfiles'

# Traverse directory to process files
filepath = os.path.join(os.getcwd(),'files/out-of-network')
dir_list = os.listdir(filepath)

combined_df_list = []
counter = 1

# Extract Data for each file in the directory
for file in dir_list:
    file_start_time = time.time()
    #state_name = re.search('UnitedHealthcare-of-(.*)--', file).group(1)

    fullfilepath = os.path.join(filepath,file)
    with open(fullfilepath) as f:
        dat = f.read()
        json_data = json.loads(dat)

    try: 

        # Extracting out-of-network rates data
        reporting_entity_name = json_data['reporting_entity_name']
        reporting_entity_type = json_data['reporting_entity_type']
        last_updated_on = json_data['last_updated_on']
        version = json_data['version']

        print(f"\nTask Started : Parsing Out-Network Rates Data For File : {file}\n")

        out_network_data = []
        for x in range (0,len(json_data['out_of_network'])-1):
            for allowed_amount in json_data['out_of_network'][x]['allowed_amounts']:
                for payment in allowed_amount['payments']:
                    for provider in payment['providers']:
                            for npi in provider['npi']:
                                out_of_network_dict = {
                                'reporting_entity_name' : reporting_entity_name,
                                'reporting_entity_type' : reporting_entity_type,
                                'last_updated_on' : last_updated_on,
                                'version' : version,
                                'name' : json_data['out_of_network'][x]['name'],
                                'billing_code_type' : json_data['out_of_network'][x]['billing_code_type'],
                                'billing_code_type_version' : json_data['out_of_network'][x]['billing_code_type_version'],
                                'billing_code' : json_data['out_of_network'][x]['billing_code'], 
                                'description' :   json_data['out_of_network'][x]['description'],
                                'tin_type': allowed_amount['tin']['type'],
                                'tin_value': allowed_amount['tin']['value'],
                                'service_code': ','.join(map(str, allowed_amount['service_code'])),
                                'billing_class' : allowed_amount['billing_class'],
                                'allowed_amount' : payment['allowed_amount'],
                                'billed_charge' : provider['billed_charge'],
                                'npi' : round(npi)
                                #npi['npi'].str
                                }
                                out_network_data.append(out_of_network_dict)

        # Creating DataFrame to store out-network provider data   
        df = pd.DataFrame(out_network_data)


        print(f"\nTask Started : Writing to CSV files For File : {file}\n")

        #df1.to_csv(outputdirectory + '\\providerreferences_' + state_name + '_file_' + str(counter) + '.csv')
        #df2.to_csv(outputdirectory + '\\innetworkrates_' + '_file_' + str(counter) + '.csv')

        df.to_csv(outputdirectory + '\\provider_out-networkrates_file_' + str(counter) + '.csv')

        print(f"\nTask Completed : Writing to CSV files For File : {file}\n")

    except Exception as e: 
        print(e)
        raise

    finally:
        f.close()
        file_end_time = time.time()
        elapsed_time = file_end_time - file_start_time
        print(f"\n--- Parsed File {file} in Time: {elapsed_time:.2f} seconds ---")

end_time = time.time() 
total_elapsed_time = end_time-start_time
print(f"\n--- Total Execution Time: {total_elapsed_time:.2f} seconds ---\n")
    