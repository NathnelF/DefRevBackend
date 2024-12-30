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
import argparse




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

def month_to_number(month):
    numeric = 0
    if month == "January" or month == "january":
        numeric = 1
    elif month == "February" or month == "february":
        numeric = 2
    elif month == "March" or month == "march":
        numeric = 3
    elif month == "April" or month == "april":
        numeric = 4
    elif month == "May" or month == "may":
        numeric = 5
    elif month == "June" or month == "june":
        numeric = 6
    elif month == "July" or month == "July":
        numeric = 7
    elif month == "August" or month == "august":
        numeric = 8
    elif month == "September" or month == "september":
        numeric = 9
    elif month == "October" or month == "october":
        numeric = 10
    elif month == "November" or month == "november":
        numeric = 11
    elif month == "December" or month == "december":
        numeric = 12
    else:
        return print("Error: Enter a valid month.")
    return numeric

def split_date(date):
    date = date.split("/")
    month = int(date[0])
    year = date[2]
    return month, year

def monthly_report(month, year):
    try:
        new_worksheet = MasterSheet.worksheet("Amalgamated Data")
        numeric = month_to_number(month)
        dates = new_worksheet.col_values(2)[1:]
        customers = new_worksheet.col_values(1)[1:]
        check = []
        entries = []
        for index, date in enumerate(dates):
            m,y = split_date(date)
            if (m == numeric and y == year):
                row = index + 2
                print(row)
                entry = new_worksheet.row_values(row)
                print(entries)
                entries.append(entry)
            

        headers = new_worksheet.row_values(1)

        if not entries:
            return print(f"No entries found for {month} {year}. No new worksheet created.")
        
        try:
            worksheet = MasterSheet.worksheet(f"{month} report for {year}")
            worksheet.clear()
            worksheet.insert_row(headers,1)
            worksheet.insert_rows(entries, 2)  
        except gspread.exceptions.WorksheetNotFound:
            worksheet = MasterSheet.add_worksheet(title=f"{month} report for {year}", rows=100, cols=20)
            worksheet.insert_row(headers,1)
            worksheet.insert_rows(entries, 2)  # Insert starting from row 2 to preserve header if needed
            print(f"Worksheet '{month} report for {year}' created and updated with entries.")

    except gspread.exceptions.WorksheetNotFound:
        return print("No data to work with")
    
    return print("All done")


parser = argparse.ArgumentParser(description='command-line arguments.')
parser.add_argument("Month", type=str, help="Generate report for this month")
parser.add_argument("Year", type=str, help="Process report for month in this year")
args = parser.parse_args()


monthly_report(args.Month, args.Year)