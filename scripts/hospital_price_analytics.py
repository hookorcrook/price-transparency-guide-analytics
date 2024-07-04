import pandas as pd
import pygwalker as pyg

# Load the data from the CSV file
outputdirectory = 'outputfiles'
data1 = pd.read_csv(outputdirectory+'\\hospital-price-transparency_standardized_JSON.csv')
data1['Organization'] = 'Northwestern Memorial Health'
data2 = pd.read_csv(outputdirectory+'\\Rush-University-Medical-Centerhospital-price-transparency_standardized_CSV.csv')
data2['Organization'] = 'Rush University Medical Center'

# Merge two dataframes
data = pd.concat([data1,data2])


# Procedure Code Sets to be processed
procedure_code_sets = {
    'MRI Codes': ['70540' ,'70542' ,'70543' ,'70551' ,'70552' ,'70553' ,'70554' ,'70555' ,'70557' ,'70558' ,'70559' ,'71550' ,'71551' ,'71552' ,'71555' ,'72141' ,'72142' ,'72146' ,'72147' ,'72148' ,'72149' ,'72156' ,'72157' ,'72158' ,'72195' ,'72196' ,'72197' ,'73218' ,'73219' ,'73220' ,'73221' ,'73222' ,'73223' ,'73718' ,'73719' ,'73720' ,'73721' ,'73722' ,'73723' ,'74181' ,'74182' ,'74183' ,'74185' ,'75557' ,'75561' ,'75563' ,'75565'],
    'XRay Codes': ['70100' ,'70110' ,'70130' ,'70140' ,'70150' ,'70160' ,'70200' ,'70210' ,'70220' ,'70250' ,'70260' ,'70330' ,'70360' ,'70390' ,'71045' ,'71046' ,'71047' ,'71048' ,'71100' ,'71101' ,'71110' ,'71111' ,'71120' ,'72020' ,'72040' ,'72050' ,'72052' ,'72070' ,'72072' ,'72080' ,'72081' ,'72082' ,'72083' ,'72084' ,'72100' ,'72110' ,'72114' ,'72170' ,'72190' ,'72202' ,'72220' ,'73000' ,'73010' ,'73020' ,'73030' ,'73050' ,'73060' ,'73070' ,'73080' ,'73090' ,'73092' ,'73100' ,'73110' ,'73120' ,'73130' ,'73140' ,'73501' ,'73502' ,'73503' ,'73521' ,'73522' ,'73523' ,'73551' ,'73552' ,'73560' ,'73562' ,'73564' ,'73565' ,'73590' ,'73592' ,'73600' ,'73610' ,'73620' ,'73630' ,'73650' ,'73660' ,'74018' ,'74019' ,'74021' ,'74022' ,'74190' ,'74470' ,'76080' ,'76098'],
    'Blood Test Codes': ['80047' ,'80048' ,'80051' ,'80053' ,'80061' ,'80069' ,'80076' ,'82103' ,'82104' ,'84443' ,'85610' ,'85611' ,'85730' ,'85732'],
    # Add more sets as needed
}

# Filter the data to include only the specified procedure code sets
filtered_data = pd.DataFrame()
data['Code'] = data['Code'].astype(str)
for set_name, codes in procedure_code_sets.items():
    set_data = data[data['Code'].isin(codes)].copy()
    set_data['ProcedureSet'] = set_name  # Add a column to indicate the set
    filtered_data = pd.concat([filtered_data, set_data])

# Clean up data to remove non-payer rates
filtered_data = filtered_data[~filtered_data['Payer'].isin(['Cash Rate', 'List Price','GIFT OF HOPE [5300]_GIFT OF HOPE [530010]'])]



# Group the data by Payer and ProcedureSet, and calculate the average rate
#grouped_data = filtered_data.groupby(['Payer', 'ProcedureSet']).agg(Average_Rate=('Rate', 'mean')).reset_index()
grouped_data = filtered_data.groupby(['Organization','ProcedureSet']).agg(Average_Rate=('Rate', 'mean')).reset_index()

# Generate an interactive report using PyGWalker
gwalker = pyg.walk(grouped_data)
#gwalker = pyg.walk(filtered_data)
# Save the generated HTML content to a file
with open(outputdirectory+'\\interactive_report_5.html', 'w') as f:
    f.write(gwalker.to_html())

print("Interactive report generated and saved as 'interactive_report_5.html'")
