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


def delete_sheets(sheet_list_titles):
    for title in sheet_list_titles:
        if "recognition schedule" in title:
            sh = MasterSheet.worksheet(title)
            MasterSheet.del_worksheet(sh)
            print(f"{title} succcessfully cleared")
        else:
            continue
    return print("All done")

def clear_sched():
    length = len(contracts.col_values(1)[1:])
    cells = []
    for x in range(2, length+2):
        cells.append(Cell(x, 12, ''))
    contracts.update_cells(cells, value_input_option='USER_ENTERED') #batch update the cells
    return print("All done")


clear_sched()
delete_sheets(sheet_list_titles)