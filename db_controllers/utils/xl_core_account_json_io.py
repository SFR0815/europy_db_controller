import pandas as pd
import json, os, openpyxl

def excel_to_json_dict(excel_file_path, sheet_name=0):
    # Read the Excel file
    df = pd.read_excel(excel_file_path, sheet_name=sheet_name, header=0, index_col=0)
    
    # Convert DataFrame to dictionary
    result = {}
    for index, row in df.iterrows():
        # return result if index is NaN
        if pd.isna(index):
          return result
        
        # Create a dictionary for each row, including None for empty values
        row_dict = {col: None if pd.isna(val) else val for col, val in row.items()}
        
        # Add to result dictionary
        result[str(index)] = row_dict

    return result

def json_to_excel(json_file_path, output_excel_path, sheet_name='Sheet1'):
    # Read the JSON file
    with open(json_file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Convert JSON to DataFrame
    df = pd.DataFrame.from_dict(data, orient='index')
    
    # Create a new workbook and save DataFrame to Excel
    workbook = openpyxl.Workbook()
    with pd.ExcelWriter(output_excel_path, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=True)
    
    # print(f"New Excel file created and saved to {output_excel_path}")




def save_json(data, output_file_path):
    with open(output_file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


