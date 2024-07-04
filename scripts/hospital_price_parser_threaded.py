import pandas as pd
import json
import re
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed

class HospitalFileParser:
    def __init__(self, file_path):
        self.file_path = file_path
        self.file_type = self.detect_file_type()

    def detect_file_type(self):
        if self.file_path.endswith('.csv'):
            return 'csv'
        elif self.file_path.endswith('.xlsx'):
            return 'xlsx'
        elif self.file_path.endswith('.json'):
            return 'json'
        elif self.file_path.endswith('.xml'):
            return 'xml'
        else:
            raise ValueError("Unsupported file type")

    def parse(self):
        if self.file_type == 'csv':
            return self.parse_csv()
        elif self.file_type == 'xlsx':
            return self.parse_xlsx()
        elif self.file_type == 'json':
            return self.parse_json()
        elif self.file_type == 'xml':
            return self.parse_xml()

    def parse_csv(self):
        return pd.read_csv(self.file_path)

    def parse_xlsx(self):
        xls = pd.ExcelFile(self.file_path)
        data_frames = []
        for sheet_name in xls.sheet_names:
            df = pd.read_excel(xls, sheet_name=sheet_name)
            df['Sheet_Name'] = sheet_name  # Add the sheet name as a column to track source
            data_frames.append(df)
        return pd.concat(data_frames, ignore_index=True)

    def parse_json(self):
        try:
            with open(self.file_path, 'r', encoding='utf-8-sig') as f:
                data = f.read()
                if data.startswith('\ufeff'):
                    data = data.encode('utf-8-sig').decode('utf-8-sig')
                data = json.loads(data)

            dfs = []
            for key, value in data.items():
                if isinstance(value, list):
                    df = pd.json_normalize(value)
                    df['File_Date'] = key  # Add a column to track the source key
                    dfs.append(df)

            if not dfs:
                raise ValueError("No array data found in the JSON file.")
            
            combined_df = pd.concat(dfs, ignore_index=True)
            return combined_df

        except FileNotFoundError:
            print(f"Error: File '{self.file_path}' not found.")
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON in file '{self.file_path}': {e}")
        except Exception as e:
            print(f"An error occurred: {e}")

    def parse_xml(self):
        tree = ET.parse(self.file_path)
        root = tree.getroot()
        data = self.xml_to_dict(root)
        return pd.json_normalize(data)

    def xml_to_dict(self, element):
        data = {}
        for child in element:
            data[child.tag] = self.xml_to_dict(child) if len(child) > 0 else child.text
        return data

class HospitalDataStandardizer:
    def __init__(self, data, file_name):
        self.data = data
        self.file_name = file_name

    def standardize(self):
        standardized_data = []
        pattern = r'_(.*?)_'
        match = re.search(pattern, self.file_name)
        source_file_name = match.group(1) if match else 'Unknown'

        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(self.standardize_row, row, source_file_name) for index, row in self.data.iterrows()]
            for future in as_completed(futures):
                standardized_data.extend(future.result())

        return pd.DataFrame(standardized_data)

    def standardize_row(self, row, source_file_name):
        standardized_entries = []
        for column in row.index:
            if self.is_payer_column(column):
                code = self.get_code(row)
                code_type = self.determine_code_type(row)
                standardized_entry = {
                    'SourceName': source_file_name,
                    'Code': self.clean_code(code),
                    'CodeType': code_type,
                    'Description': self.get_description(row),
                    'RevCode': self.get_rev_code(row),
                    'Payer': self.clean_payer(column),
                    'Rate': self.clean_rate(row[column])
                }
                standardized_entries.append(standardized_entry)
        return standardized_entries

    def is_payer_column(self, column_name):
        non_payer_columns = [
            # NWH
            'Facility', 'Billing Code', 'Billing_Code', 'HCPCs', 'CPT/HCPCs', 'Code Description', 'Service_Description',
            'Supply Code', 'Supply Name', 'Medication Name', 'Revenue Code', 'Rev Code', 'Revenue_Code',
            'Deidentified_Minimum_Negotiated_Charge', 'Deidentified_Maximum_Negotiated_Charge', 'NDC',
            'Medication ID', 'Sheet_Name', 'File_Date',
            # RUSH
            'description', 'code|1', 'code|1|type', 'code|2', 'code|2|type', 'code|3', 'code|3|type', 'code|4', 'code|4|type', 'setting', 'drug_unit_of_measurement',
            'drug_type_of_measurement', 'payer_name', 'plan_name', 'modifiers', 'standard_charge|min', 'standard_charge|max', 'standard_charge|negotiated_dollar',
            'standard_charge|negotiated_percentage', 'standard_charge|negotiated_algorithm', 'estimated_amount',
            'standard_charge|methodology', 'additional_generic_notes', 'additional_payer_notes'
        ]
        return column_name not in non_payer_columns

    def get_code(self, row):
        if 'code|2' in row and pd.notna(row['code|2']):
            return row['code|2']
        for column in ['Billing_Code', 'HCPCs', 'CPT/HCPCs']:
            if column in row and pd.notna(row[column]):
                return row[column]
        return ''

    def determine_code_type(self, row):
        if 'code|2' in row and pd.notna(row['code|2']) and 'code|2|type' in row and pd.notna(row['code|2|type']):
            return row['code|2|type']
        
        code = self.get_code(row)
        if 'MS-DRG' in code:
            return 'MS-DRG'
        if 'Custom' in code:
            return 'Custom'
        if 'ADA' in code:
            return 'ADA'
        if 'HCPCS' in code or 'CPT' in code:
            return 'HCPCS_CPT'
        return ''

    def clean_code(self, code):
        if not isinstance(code, str):
            code = str(code)
        
        if 'MS-DRG' in code:
            code = code.replace('MS-DRG', '').strip()
        if 'HCPCS' in code:
            code = code.replace('HCPCS', '').strip()
        if 'CPT' in code or 'CPT®' in code:
            code = code.replace('CPT', '').replace('CPT®', ''). strip()
        if 'Custom' in code:
            code = code.replace('Custom', '').strip()
        if 'ADA' in code:
            code = code.replace('ADA', '').strip()
        if '(' in code:
            code = code.split('(')[0].strip()
    
        code_parts = code.split()
        if code_parts:
            return code_parts[-1][:5]
        else:
            return code[:5] if len(code) > 5 else code

    def get_description(self, row):
        for column in ['Code Description', 'Supply Name', 'Medication Name', 'Service_Description', 'description']:
            if column in row and pd.notna(row[column]):
                return row[column]
        return ''

    def get_rev_code(self, row):
        for column in ['Revenue Code', 'Rev Code', 'Revenue_Code']:
            if column in row and pd.notna(row[column]):
                return row[column]
        return ''

    def clean_payer(self, payer):
        if 'Discounted_Cash_Price' in payer or 'standard_charge|discounted_cash' in payer:
            return 'Cash Rate'
        elif 'Gross Charge' in payer or 'Gross_Charge' in payer or 'standard_charge|gross' in payer:
            return 'List Price'
        else:
            return payer.replace('Negotiated Charge:', '').strip()
        return ''

    def clean_rate(self, rate):
        if isinstance(rate, str):
            rate = rate.replace('$', '').replace(',', '').strip()
        try:
            return float(rate)
        except ValueError:
            return 0.0
