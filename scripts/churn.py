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
from lifecycle_complete import complete
from renewal import update_recognition_schedule
from renewal import get_lifecycle_fields
from recognition_schedule import split_date
from recognition_schedule import increment_date
from recognition_schedule import get_recognition_schedule_values

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
scheduled_list = []

sheet_list = MasterSheet.worksheets()
sheet_list_titles = get_sheet_list_titles(sheet_list)
contracts = MasterSheet.worksheet("Contracts")
lifecycle = MasterSheet.worksheet("Contract Lifecycle Events")

if __name__ == "__main__":
# how should this trigger.
    churn_dates = contracts.col_values(10)[1:]
    for i, churn_date in enumerate(churn_dates):
            if churn_date:
                 customer = contracts.acell(f"A{i+2}").value
                 service = contracts.acell(f"B{i+2}").value
                 print(f"{customer} churned on {churn_date} found at row: {i+2}")   
                 title = f"{customer} {service} recognition schedule"
                 sheet = MasterSheet.worksheet(title)
                 sheet_dates = sheet.col_values(2)[1:]
                 length = len(sheet_dates)
                 final_date = sheet_dates[length-1]
                 churn_datetime = datetime.strptime(churn_date, "%m/%d/%Y")
                 final_datetime = datetime.strptime(final_date, "%m/%d/%Y")
                 if (churn_datetime >= final_datetime):
                    print("contract churned at the end of lifecycle")
                 else:
                    print("contract churned in middle of lifecycle")
                    churn_m, churn_y = split_date(churn_date)
                    for index, sheet_date in enumerate(sheet_dates):
                        m,y = split_date(sheet_date)
                        if m == churn_m and y == churn_y:
                            print("Churn line found")
                            churn_line = index + 2
                            clear_line = churn_line + 1
                            total_len = len(sheet_dates) + 1
                            range = [f"A{clear_line}:F{total_len}"]
                            sheet.batch_clear(range)


                    #find line where churn date aligns with recoginition date
                    # delete everything after that line
                 
