

# Using file parser and standardizer class
workingdirectory = 'C:\\Users\\mesha\Downloads'
file_path = workingdirectory + '\\363484281_northwestern-memorial-hospital_standardcharges.xlsx'  # Pass file path here
#file_path = workingdirectory + '\\37-0960170_Northwestern Memorial Hospital_standardcharges.json'
parser = HospitalFileParser(file_path)
parsed_data = parser.parse()

# Standardize data based on target columns
standardizer = HospitalDataStandardizer(parsed_data, file_path)
standardized_data = standardizer.standardize()
standardized_data.drop_duplicates(inplace=True)

print(standardized_data)
standardized_data.to_csv(workingdirectory + '\\hospital-price-transparency_standardized_EXCEL.csv',index=False)
#standardized_data.to_csv(workingdirectory + '\\hospital-price-transparency_standardized_JSON.csv',index=False)
