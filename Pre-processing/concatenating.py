#Module that concatenates monthly accounting data into a single file. 

import pandas as pd
import os
import re


def combine_accounting_sheets(file_path, output_path):
    xls = pd.ExcelFile(file_path)
    combined_data = []
    
    for sheet_name in xls.sheet_names:
        if len(sheet_name) == 6 and sheet_name.isdigit():
            year = int(sheet_name[:4])
            month = int(sheet_name[4:])
        else:
            print(f"Skipping sheet {sheet_name} as it does not follow the naming convention")
            continue
        df = pd.read_excel(xls, sheet_name=sheet_name, skiprows=1)
                
        if "Dato:" not in df.columns:
            print(f"Skipping sheet {sheet_name} as it does not contain a 'Date:' column")
            continue

        df = df.drop_duplicates()
        df = df[df["Dato:"].apply(lambda x: bool(re.match(r'^(3[01]|[12][0-9]|0?[1-9])$', str(x))))]
        df["Date"] = pd.to_datetime(df["Dato:"].apply(lambda d: f"{d}/{month}/{year}"), format="%d/%m/%Y", errors='coerce')

        df = df[["Date", "Totalt salg"]]
        
        combined_data.append(df)
    
    full_df = pd.concat(combined_data, ignore_index=True)
    full_df = full_df.groupby("Date", as_index=False).sum()
    full_df.to_excel(output_path, index=False)
    print(f"Combined file saved to {output_path}")

file_path = "Data/kasseoppgjør_2025.xlsx" 
output_path = "Data/concat_revenue2.xlsx"
combine_accounting_sheets(file_path, output_path)