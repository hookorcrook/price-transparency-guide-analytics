from hospital_price_parser import HospitalFileParser,HospitalDataStandardizer
#from hospital_price_parser_threaded import HospitalFileParser,HospitalDataStandardizer
import os
import time
import re



def main():
    start_time = time.time()

# Provide folder path to generate output files
    outputdirectory = 'outputfiles'

# Traverse directory to process files
    filepath = os.path.join(os.getcwd(),'files/hospital-prices')
    dir_list = os.listdir(filepath)
    for file in dir_list:
        file_start_time = time.time()
        fullfilepath = os.path.join(filepath,file)
        parser = HospitalFileParser(fullfilepath)
        parsed_data = parser.parse()

    # Standardize data based on target columns
        standardizer = HospitalDataStandardizer(parsed_data, fullfilepath)
        standardized_data = standardizer.standardize()
        standardized_data.drop_duplicates(inplace=True)

        print(standardized_data)
        pattern = r'_(.*?)_'
        match = re.search(pattern, file)
        org_name = match.group(1) if match else 'Unknown' 

        print(f"\nTask Started : Writing to output CSV file for input file : {file} \n")
        if parser.file_type == 'xlsx':
            standardized_data.to_csv(outputdirectory + '\\' +org_name+ 'hospital-price-transparency_standardized_EXCEL.csv',index=False)
        if parser.file_type == 'json':
             standardized_data.to_csv(outputdirectory + '\\' +org_name+ 'hospital-price-transparency_standardized_JSON.csv',index=False)
        if parser.file_type == 'csv':
             standardized_data.to_csv(outputdirectory + '\\' +org_name+ 'hospital-price-transparency_standardized_CSV.csv',index=False)
        if parser.file_type == 'xml':
             standardized_data.to_csv(outputdirectory + '\\' +org_name+ 'hospital-price-transparency_standardized_XML.csv',index=False)
        else:
            pass
        print(f"\nTask Complete : Writing to output CSV file for input file : {file} \n")
        
    end_time = time.time() 
    total_elapsed_time = end_time-start_time
    print(f"\n--- Total Execution Time: {total_elapsed_time:.2f} seconds ---\n")

if __name__ == "__main__":
    main()
