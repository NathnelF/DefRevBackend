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

def create_datasheet(month, csv):
    if sheet_exists(f"Quickbooks Data {month}") == False:
        qbdata = MasterSheet.add_worksheet(title=f"Quickbooks Data {month}", rows=100, cols=20)
    else:
        qbdata = MasterSheet.worksheet(f"Quickbooks Data {month}")

    df = pd.read_csv(csv)
    # headers = df.columns.to_list()
    # print(headers)
    columns_to_keep = ['Date', 'Memo/Description', 'Amount', 'Balance']
    columns_to_keep_invoice = ['Date', 'Transaction type', 'Name', 'Memo/Description', 'Amount', 'Balance']
    # invoices = df[df['Transaction type'] == 'Invoice']
    # invoices = invoices[columns_to_keep_invoice]
    # jentriess = df[df['Transaction type'] == "Journal Entry"]
    # jentriess = jentriess[columns_to_keep]
    # print(invoices)
    # print(jentriess)
    df = df[columns_to_keep_invoice].dropna(subset=['Date'])
    df['Amount'] = pd.to_numeric(df['Amount'].astype(str).str.replace(',', '', regex=True).str.strip(), errors='coerce').fillna(0)  # Convert to float
    df['Balance'] = pd.to_numeric(df['Balance'].astype(str).str.replace(',', '', regex=True).str.strip(), errors='coerce').fillna(0)
    df.fillna('', inplace=True)
    qbdata.update([df.columns.values.tolist()] + df.values.tolist())
    qbdata.format('E2:F99', {"numberFormat": {"type": "CURRENCY"}})




parser = argparse.ArgumentParser(description="Command line arguments")
parser.add_argument("Month", type=str, help="Month to create qb report for")
parser.add_argument("Year", type=str, help="Year to process qb data in")
args = parser.parse_args()

try:
    data = f"C:\\Users\\natef\\OneDrive\\Desktop\\Projects\\NWGDefRev\\backend\\reports\\{args.Month}_{args.Year}.csv"
    if os.path.exists(data):
        create_datasheet(args.Month, data)
    else:
        raise FileNotFoundError()
except FileNotFoundError as e:
    print(f"Error: {e}")
    print("You likely entered an invalid month or year. Check if the corresponding report exists.")
except Exception as e:
    print("An unexpected error ocurred: {e}")