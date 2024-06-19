import pandas as pd
import json
import os
import time
import re

class DataProcessor:
    def __init__(self, output_directory='outputfiles'):
        self.output_directory = output_directory
        self.start_time = time.time()

    def process_in_network_files(self, filepath, generate_combined_file=False):
        dir_list = os.listdir(filepath)
        combined_df_list = []
        counter = 1

        for file in dir_list:
            file_start_time = time.time()
            state_name = re.search('UnitedHealthcare-of-(.*)--', file).group(1)

            fullfilepath = os.path.join(filepath, file)
            with open(fullfilepath) as f:
                dat = f.read()
                json_data = json.loads(dat)

            try:
                provider_data = []
                reporting_entity_name = json_data['reporting_entity_name']
                reporting_entity_type = json_data['reporting_entity_type']
                last_updated_on = json_data['last_updated_on']
                version = json_data['version']

                print(f"\nTask Started: Parsing In-Network Rates Data For File : {file}\n")

                for provider_ref in json_data['provider_references']:
                    for provider_group in provider_ref['provider_groups']:
                        for npi in provider_group['npi']:
                            provider_info = {
                                'reporting_entity_name': reporting_entity_name,
                                'reporting_entity_type': reporting_entity_type,
                                'last_updated_on': last_updated_on,
                                'version': version,
                                'npi': npi,
                                'tin_type': provider_group['tin']['type'],
                                'tin_value': provider_group['tin']['value'],
                                'provider_group_id': provider_ref['provider_group_id']
                            }
                            provider_data.append(provider_info)

                df1 = pd.DataFrame(provider_data)

                in_network_data = []
                for in_network_item in json_data['in_network']:
                    for negotiated_rates in in_network_item['negotiated_rates']:
                        for neg_data in negotiated_rates['negotiated_prices']:
                            for provider_reference in negotiated_rates['provider_references']:
                                in_network_dict = {
                                    'negotiation_arrangement': in_network_item['negotiation_arrangement'],
                                    'name': in_network_item['name'].strip(),
                                    'billing_code_type': in_network_item['billing_code_type'],
                                    'billing_code_type_version': in_network_item['billing_code_type_version'],
                                    'billing_code': in_network_item['billing_code'],
                                    'description': in_network_item['description'].strip(),
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

                df2 = pd.DataFrame(in_network_data)

                df = pd.merge(df1[['reporting_entity_name', 'reporting_entity_type', 'npi', 'provider_group_id']],
                              df2[['name', 'billing_code_type', 'billing_code', 'billing_code_type_version',
                                   'negotiation_arrangement', 'negotiated_prices', 'negotiated_type',
                                   'expiration_date', 'service_code', 'billing_class', 'provider_references']],
                              how='inner',
                              left_on='provider_group_id',
                              right_on='provider_references').drop(columns='provider_references')

                csv_filename = f'provider_in-networkrates_{state_name}_file_{counter}.csv'
                csv_filepath = os.path.join(self.output_directory, csv_filename)
                df.to_csv(csv_filepath)

                print(f"\nTask Completed : Writing to CSV files For File : {file}\n")

                combined_df_list.append(df)

                counter += 1

            except Exception as e:
                print(e)
                raise

            finally:
                f.close()
                file_end_time = time.time()
                elapsed_time = file_end_time - file_start_time
                print(f"\n--- Parsed File {file} in Time: {elapsed_time:.2f} seconds ---")

        if generate_combined_file and combined_df_list:
            self._generate_combined_file(combined_df_list, 'provider_in-networkrates_combinedfile.csv')

        self._print_total_execution_time()

    def process_out_of_network_files(self, filepath):
        dir_list = os.listdir(filepath)
        counter = 1

        for file in dir_list:
            file_start_time = time.time()

            fullfilepath = os.path.join(filepath, file)
            with open(fullfilepath) as f:
                dat = f.read()
                json_data = json.loads(dat)

            try:
                reporting_entity_name = json_data['reporting_entity_name']
                reporting_entity_type = json_data['reporting_entity_type']
                last_updated_on = json_data['last_updated_on']
                version = json_data['version']

                print(f"\nTask Started : Parsing Out-Network Rates Data For File : {file}\n")

                out_network_data = []
                for out_of_network_item in json_data['out_of_network']:
                    for allowed_amount in out_of_network_item['allowed_amounts']:
                        for payment in allowed_amount['payments']:
                            for provider in payment['providers']:
                                for npi in provider['npi']:
                                    out_of_network_dict = {
                                        'reporting_entity_name': reporting_entity_name,
                                        'reporting_entity_type': reporting_entity_type,
                                        'last_updated_on': last_updated_on,
                                        'version': version,
                                        'name': out_of_network_item['name'],
                                        'billing_code_type': out_of_network_item['billing_code_type'],
                                        'billing_code_type_version': out_of_network_item['billing_code_type_version'],
                                        'billing_code': out_of_network_item['billing_code'],
                                        'description': out_of_network_item['description'],
                                        'tin_type': allowed_amount['tin']['type'],
                                        'tin_value': allowed_amount['tin']['value'],
                                        'service_code': ','.join(map(str, allowed_amount['service_code'])),
                                        'billing_class': allowed_amount['billing_class'],
                                        'allowed_amount': payment['allowed_amount'],
                                        'billed_charge': provider['billed_charge'],
                                        'npi': round(npi)
                                    }
                                    out_network_data.append(out_of_network_dict)

                df = pd.DataFrame(out_network_data)

                csv_filename = f'provider_out-networkrates_file_{counter}.csv'
                csv_filepath = os.path.join(self.output_directory, csv_filename)
                df.to_csv(csv_filepath)

                print(f"\nTask Completed : Writing to CSV files For File : {file}\n")

                counter += 1

            except Exception as e:
                print(e)
                raise

            finally:
                f.close()
                file_end_time = time.time()
                elapsed_time = file_end_time - file_start_time
                print(f"\n--- Parsed File {file} in Time: {elapsed_time:.2f} seconds ---")

        self._print_total_execution_time()

    def _generate_combined_file(self, combined_df_list, filename):
        print(f"\nTask Started : Writing to Combined CSV file For {len(combined_df_list)} files\n")
        combined_df = pd.concat(combined_df_list)
        combined_csv_filepath = os.path.join(self.output_directory, filename)
        combined_df.to_csv(combined_csv_filepath)
        print(f"\nTask Completed : Writing to Combined CSV files For {len(combined_df_list)} files\n")

    def _print_total_execution_time(self):
        end_time = time.time()
        total_elapsed_time = end_time - self.start_time
        print(f"\n--- Total Execution Time: {total_elapsed_time:.2f} seconds ---\n")
