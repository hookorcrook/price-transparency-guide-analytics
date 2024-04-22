import pandas as pd
import json
from pandas import DataFrame as df
import os
import time

start_time = time.time()


file_name = '2024-04-01_UnitedHealthcare-of-Wisconsin--Inc-_Insurer_OHPH-Acupuncture-Massage-Naturopath_31_in-network-rates.json'
fp = os.path.join(os.getcwd(),'files',file_name)

# Traverse directory to process files
#filepath = os.path.join(os.getcwd(),'files')
#dir_list = os.listdir(filepath)

outputdirectory = 'outputfiles'

with open(fp) as f:
    dat = f.read()
    json_data = json.loads(dat)

try: 
# Extracting provider data

    print('Task Started: Parsing Provider Reference Data\n')

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
        x+=1
    
    print('Task Completed : Parsing Provider Reference Data\n')
    # Creating DataFrame to store provider data
    df1 = pd.DataFrame(provider_data)

    # Extracting in-network rates data
    print('Task Started : Parsing In-Network Rates Data\n')

    in_network_data = []
    for in_network in json_data['in_network']:
        for y in range (0,len(json_data['in_network'])-1):
        #for y in range (0,50):
            for negotiated_rates in json_data['in_network'][y]['negotiated_rates']:
                for neg_data in negotiated_rates['negotiated_prices']:
                    in_network_dict = {
                        'negotiation_arrangement':in_network['negotiation_arrangement'],
                        'name':in_network['name'].strip(),
                        'billing_code_type':in_network['billing_code_type'],
                        'billing_code_type_version': in_network['billing_code_type_version'],
                        'billing_code': in_network['billing_code'],
                        'description':in_network['description'].strip(),
                        'provider_references': negotiated_rates['provider_references'][0],
                        'negotiated_prices': neg_data['negotiated_rate'],
                        'service_code': neg_data['service_code'][0], 
                        'negotiated_type': neg_data['negotiated_type'],
                        'expiration_date': neg_data['expiration_date'],
                        'billing_class': neg_data['billing_class'],
                        'billing_code_modifier': neg_data['billing_code_modifier'],
                        'additional_information': neg_data['additional_information'].strip()
                    }
                    in_network_data.append(in_network_dict)
        y+=1
  
    print('Task Completed : Parsing In-Network Rates Data\n')
    # Creating DataFrame to store provider data   
    df2 = pd.DataFrame(in_network_data)


    # Output Files
    print('Task Started : Writing to CSV files\n')

    df1.to_csv(outputdirectory + '\\providerreferences.csv')
    df2.to_csv(outputdirectory + '\\innetworkrates.csv')

    print('Task Completed : Writing to CSV files\n')

except Exception as e: 
    print(e)
    raise

finally:
    f.close()
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"--- Execution Time: {elapsed_time:.2f} seconds ---")