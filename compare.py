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
from fuzzywuzzy import fuzz
from fuzzywuzzy import process



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

MasterSheet = client.open_by_key(sheet_id)
sheet_list = MasterSheet.worksheets()
sheet_list_titles = get_sheet_list_titles(sheet_list)


def title_exists(sheet_title, title_list):
    found = False
    for title in title_list:
        if title == sheet_title:
            sheet = MasterSheet.worksheet(title)
            found = True
            
    return found

def internal_map(month, year, title_list):
    interntaldict = {}
    sheet_title = f"{month} report for {year}"
    if title_exists(sheet_title, title_list) == True:       
        sheet = MasterSheet.worksheet(sheet_title)
    else:
        return print("No such title exists")
        
    customers = sheet.col_values(1)[1:]
    revenues = sheet.col_values(3)[1:]

    for x in range(0,len(customers)):
        interntaldict[customers[x]] = revenues[x]
    
    return interntaldict

def qb_map(month, title_list):
    qbdict = {}
    sheet_title = f"Quickbooks Data {month}"
    if title_exists(sheet_title, title_list) == True:       
        sheet = MasterSheet.worksheet(sheet_title)
    else:
        return print("No such title exists")
            
        
    customers = sheet.col_values(4)[1:]
    names = sheet.col_values(3)[1:]
    revenues = sheet.col_values(5)[1:]
    types = sheet.col_values(2)[1:]

    for x in range(0,len(customers)):
        if types[x] == "Journal Entry":
            qbdict[customers[x]] = revenues[x]
        elif types[x] == "Invoice":
            qbdict[names[x]] = revenues[x]
        else:
            print("Invalid transaction type")

    return qbdict

def find_best_match(internal_name, qb_names):
    # Use fuzzy matching to find the best match in qb_names for the internal_name
    match, score = process.extractOne(internal_name, qb_names, scorer=fuzz.token_sort_ratio)
    return match if score > 80 else None  # Set a threshold for matching, 80 is a good starting point

def compare_maps(month, year, title_list):
    qb_sheet_title = f"Quickbooks Data {month}"
    if title_exists(qb_sheet_title, title_list):       
        qb_sheet = MasterSheet.worksheet(qb_sheet_title)
    else:
        print("No such title exists")
    
    internal_sheet_title = f"{month} report for {year}"
    if title_exists(internal_sheet_title, title_list):       
        internal_sheet = MasterSheet.worksheet(internal_sheet_title)
    else:
        print("No such title exists")
    
    internal = internal_map(month, "2024", title_list)
    qb = qb_map(month, title_list)

    internalkeys = list(internal.keys())
    print(internalkeys)
    qbkeys = list(qb.keys())
    shared = []
    print(internalkeys)
    for key in internalkeys:
        match = find_best_match(key, qbkeys)
        if match:
            print(match)
            print(internal[key])
            print(qb[match])
            shared.append(match)
            if internal[key] != qb[match]:
                print(f"Mistake found at {qb[match]}!")
                mistake_i = internal_sheet.find(internal[key])
                mistake_q = qb_sheet.find(qb[match])
                if mistake_i:
                    internal_sheet.format(f'{mistake_i.address}', {
                        "backgroundColor": {
                            "red": 1.0,
                            "green": 0.0,
                            "blue": 0.0
                        }
                    })
                else:
                    print("Could not find mistake in internal sheet.")
                if mistake_q:
                    qb_sheet.format(f'{mistake_q.address}', {
                        "backgroundColor": {
                            "red": 1.0,
                            "green": 0.0,
                            "blue": 0.0
                        }
                    })
                else:
                    print("Could not find mistake in QuickBooks sheet.")
        
            # add logic to get a shared list and then compare that list to see if any items are missing.  

def find_balance(sheet):
    rows = len(sheet.col_values(5))
    col = sheet.find("Balance").col
    cell = sheet.cell(rows, col)
    balance = cell.value
    return balance

def compare_balances(month, year, title_list):
    qb_sheet_title = f"Quickbooks Data {month}"
    if title_exists(qb_sheet_title, title_list):       
        qb_sheet = MasterSheet.worksheet(qb_sheet_title)
    else:
        print("No such title exists")
    
    internal_sheet_title = f"{month} report for {year}"
    if title_exists(internal_sheet_title, title_list):       
        internal_sheet = MasterSheet.worksheet(internal_sheet_title)
    else:
        print("No such title exists")

    qb_balance = find_balance(qb_sheet)
    print(qb_balance)
    internal_balance = find_balance(internal_sheet)
    print(internal_balance)
    if (internal_balance != qb_balance):
        qbcell = qb_sheet.find(qb_balance)
        incell = internal_sheet.find(internal_balance)
        if incell:
                    internal_sheet.format(f'{incell.address}', {
                        "backgroundColor": {
                            "red": 1.0,
                            "green": 0.0,
                            "blue": 0.0
                        }
                    })
        else:
                    print("Could not find mistake in internal sheet.")
        if qbcell:
                    qb_sheet.format(f'{qbcell.address}', {
                        "backgroundColor": {
                            "red": 1.0,
                            "green": 0.0,
                            "blue": 0.0
                        }
                    })
        else:
                    print("Could not find mistake in QuickBooks sheet.")

MasterSheet = client.open_by_key(sheet_id)
sheet_list = MasterSheet.worksheets()
sheet_list_titles = get_sheet_list_titles(sheet_list)

compare_maps("March", "2024", sheet_list_titles)
compare_balances("March", "2024", sheet_list_titles)
