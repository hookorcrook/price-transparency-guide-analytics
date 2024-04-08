import pandas as pd
import json
from pandas import DataFrame as df

file_path = 'C:\\Users\\shashwot.musyaju\\Documents\\TransparencyInCoverage\\price-transparency-guide-analytics\\files\\2024-04-01_UnitedHealthcare-of-Wisconsin--Inc-_Insurer_OHPH-Acupuncture-Massage-Naturopath_31_in-network-rates.json'
with open(file_path) as f:
    x = f.read()
    json_data = json.loads(x)
    #print(json_data)
    #df1 = df.from_dict(pd.json_normalize(json_data),orient='columns')
    #df_provider_ref = df1['provider-reference']

# Extracting provider data
provider_data = []
reporting_entity_name = json_data['reporting_entity_name']
reporting_entity_type = json_data['reporting_entity_type']
last_updated_on = json_data['last_updated_on']
version = json_data['version']
for provider_group in json_data['provider_references'][0]['provider_groups']:
    provider_info = {
        'reporting_entity_name': reporting_entity_name,
        'reporting_entity_type': reporting_entity_type,
        'last_updated_on': last_updated_on,
        'version': version,
        'npi': provider_group['npi'][0],
        'tin_type': provider_group['tin']['type'],
        'tin_value': provider_group['tin']['value']
    }
    provider_data.append(provider_info)

# Creating DataFrame
df1 = pd.DataFrame(provider_data)

print(df1)
    


# df1 = pd.read_json(file_path)
# df1.head()
# with open(file_path) as f:
#     f.readline()
#     df1 = df.from_dict(pd.json_normalize(f), orient='columns')
#     print(df1.head())
    