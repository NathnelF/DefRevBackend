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
MasterSheet = client.open_by_key(sheet_id)
sheet_list = MasterSheet.worksheets()

def sheet_exists(sheet_name):
    sheet_list = MasterSheet.worksheets()
    for sheet in sheet_list:
        if sheet.title == sheet_name:
            return True
    return False

def create_yearsheet(year, csv):
    if sheet_exists(f"Quickbooks Data {year}") == False:
        qbdata = MasterSheet.add_worksheet(title=f"Quickbooks Data {year}", rows=1000, cols=20)
    else:
        qbdata = MasterSheet.worksheet(f"Quickbooks Data {year}")

    df = pd.read_csv(csv)
    headers = df.columns.to_list()
    print(headers)
    columns_to_keep = ['Date', 'Transaction type', 'Name', 'Memo/Description', 'Amount', 'Balance']
    df = df[columns_to_keep].dropna(subset=['Date'])
    df['Amount'] = pd.to_numeric(df['Amount'].astype(str).str.replace(',', '', regex=True).str.strip(), errors='coerce').fillna(0)  # Convert to float
    df['Balance'] = pd.to_numeric(df['Balance'].astype(str).str.replace(',', '', regex=True).str.strip(), errors='coerce').fillna(0)
    df.fillna('', inplace=True)
    qbdata.update([df.columns.values.tolist()] + df.values.tolist())
    qbdata.format('E2:F999', {"numberFormat": {"type": "CURRENCY"}})
    return

def create_datasheet(month, year, csv):
    if sheet_exists(f"Quickbooks Data {year}") == True: #check if the year's worth of data has been created
        qbdata = MasterSheet.worksheet(f"Quickbooks Data {year}")
    else:
        create_yearsheet(year, csv) #if it hasn't create it
        qbdata = MasterSheet.worksheet(f"Quickbooks Data {year}")
    if sheet_exists(f"Quickbooks {month} {year}") == False:
        qb_month = MasterSheet.add_worksheet(title=f"Quickbooks {month} {year}", rows = 100, cols = 20)
    else:
        qb_month = MasterSheet.worksheet(f"Quickbooks {month} {year}")
    
    dt = datetime.strptime(f"{month} 1, {year}", "%B %d, %Y")
    print(dt.month, dt.year)
    dt_end = dt + relativedelta(months = 1)
    print(dt_end)
    dates = qbdata.col_values(1)[2:]
    entries = []
    #could implement binary search here to find month faster
    for index, date in enumerate(dates,start=3):
        if datetime.strptime(date, "%m/%d/%Y").month == dt.month and datetime.strptime(date, "%m/%d/%Y").year == dt.year:
            print("Initial match established")
            entries.append(qbdata.row_values(index))
        if datetime.strptime(date, "%m/%d/%Y") == dt_end:
            print("Stopping")
            break
    headers = qbdata.row_values(1)
    qb_month.clear()
    qb_month.insert_row(headers,1)
    qb_month.insert_rows(entries, 2) 
    qb_month.sort((4, "asc"), range="A2:F100")
    return 



#888 - 237 - 8289
#department large productive fulfillment 
#




parser = argparse.ArgumentParser(description="Command line arguments")
parser.add_argument("Timeframe", type=int, help="1 for yearly report, 2 for monthly")
parser.add_argument("Month", type=str, help="Month to create qb report for")
parser.add_argument("Year", type=str, help="Year to process qb data in")
args = parser.parse_args()

if (args.Timeframe == 2):
    try:
        data = f"C:\\Users\\natef\\OneDrive\\Desktop\\Projects\\NWGDefRev\\backend\\reports\\QBreport{args.Year}.csv"
        if os.path.exists(data):
            create_datasheet(args.Month, args.Year, data)
        else:
            raise FileNotFoundError()
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("You likely entered an invalid month or year. Check if the corresponding report exists.")
    except Exception as e:
        print("An unexpected error ocurred: {e}")
elif (args.Timeframe == 1):
    try:
        data = f"C:\\Users\\natef\\OneDrive\\Desktop\\Projects\\NWGDefRev\\backend\\reports\\QBreport{args.Year}.csv"
        if os.path.exists(data):
            create_yearsheet(args.Year, data)
        else:
            raise FileNotFoundError()
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("You likely entered an invalid month or year. Check if the corresponding report exists.")
    except Exception as e:
        print("An unexpected error ocurred: {e}")
else:
    print("Timeframe not valid")