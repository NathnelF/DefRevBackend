import os
import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import GSpreadException
from gspread.exceptions import APIError
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from gspread import Cell
import time
import logging
import pandas as pd





#Connect python script to google sheets API
CLIENT_FILE = 'desktopoauthkey.json'

scope = ["https://www.googleapis.com/auth/spreadsheets"]

creds = Credentials.from_service_account_file("credentials.json", scopes=scope)
client = gspread.authorize(creds)
sheet_id = '1LgvCXbsUdX9JHGRdvZknG6QZiNjqCRREPo0dLtX98q4'

logging.basicConfig(level=logging.INFO)
read_request_counter = 0


def get_sheet_list_titles(sheet_list):
    title_list = []
    for sheet in sheet_list:
        title_list.append(sheet.title)
    return title_list

def track_read_request():
    global read_request_counter
    read_request_counter += 1
    logging.info(f"Read request #{read_request_counter}")

#Get Master sheet
MasterSheet = client.open_by_key(sheet_id)
master_url = MasterSheet.url
#List of all sheets in the master sheet
months = [
    "January", "February", "March", "April", "May", "June", 
    "July", "August", "September", "October", "November", "December"
]
sheet_list = MasterSheet.worksheets()
sheet_list_titles = get_sheet_list_titles(sheet_list)
contracts = MasterSheet.worksheet("Contracts")
lifecycle = MasterSheet.worksheet("Contract Lifecycle Events")
            
def amalgamate_data(title_list):
    all_data = pd.DataFrame()
    for title in title_list:
        if "schedule" in title.lower():
            sheet = MasterSheet.worksheet(title)
            headers = sheet.row_values(1)
            #print(f"Headers({title}):", headers)
            data = sheet.get_all_records()
            df = pd.DataFrame(data)
            all_data = pd.concat([all_data, df])
            all_data['Date'] = pd.to_datetime(all_data['Date'])  # Set data in 'Date' column to datetime
            all_data.sort_values(by='Date', ascending=True, inplace=True) 
            all_data['Date'] = all_data['Date'].dt.strftime("%#m/%#d/%Y")

    try:
        new_worksheet = MasterSheet.worksheet("Amalgamated Data")
    except gspread.exceptions.WorksheetNotFound:
        new_worksheet = MasterSheet.add_worksheet(title="Amalgamated Data", rows="1000", cols="20")
    new_worksheet.clear()
    print(all_data)
    if all_data.isnull().values.any():
        raise ValueError("Data contains NaN values.")
    new_worksheet.update([all_data.columns.values.tolist()] + all_data.values.tolist())
    # rows = len(new_worksheet.col_values(5))
    # cells = []
    # for row in range(3, rows):
    #     formula = f"=E{row-1} + C{row}"
    #     cells.append(Cell(row,5, formula))

    # new_worksheet.update_cells(cells, value_input_option='USER_ENTERED')
    # new_worksheet.format(f'E2:E{rows}', {"numberFormat": {"type": "CURRENCY"}, "horizontalAlignment": "LEFT"})


    return print("All done!!")


amalgamate_data(sheet_list_titles)