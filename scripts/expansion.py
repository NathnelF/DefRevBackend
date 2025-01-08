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
from renewal import update_recognition_schedule
from lifecycle_complete import complete



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

def get_lifecycle_fields(sheet, row):
    event = sheet.col_values(1)
    event = event[row]
    timing = sheet.col_values(2)
    timing = timing[row]
    customer = sheet.col_values(3)
    customer = customer[row]
    service = sheet.col_values(4)
    service = service[row]
    id = sheet.col_values(5)
    id = id[row]
    effective_date = sheet.col_values(6)
    effective_date = effective_date[row]
    invoice_schedule = sheet.col_values(7)
    invoice_schedule = invoice_schedule[row]
    invoice_date = sheet.col_values(8)
    invoice_date = invoice_date[row]
    invoice_amount = sheet.col_values(9)
    invoice_amount = invoice_amount[row]
    service_term = sheet.col_values(12)
    service_term = service_term[row]
    return event, timing, customer, service, id, effective_date, invoice_schedule, invoice_date, invoice_amount, service_term

def get_date_values(date):
    date_time = datetime.strptime(date, "%m/%d/%Y") #converts string date into datetime
    m = date_time.month #month value
    d = date_time.day #day value
    y = date_time.year #year value
    return m,d,y

def find_month(m,d,y):
    if d <= 20: #cutoff day is set to the 20th
        target_month = m #if day is less than or equal to cutoff we choose effective date month as our month.
        target_year = y
    elif d > 20 and m != 12:
        target_month = m + 1 # otherwise we take use the next month
        target_year = y
    else:
        target_month = 1 #there is no month 13 so we use logic to set the following month to january if the effective was december after the 20th.
        target_year = y + 1
    return target_month, target_year

def month_lookup(target_month, target_year, sheet):
    dates = sheet.col_values(2)[1:]
    print(dates)
    for index, date in enumerate(dates):
        print(date)
        print(type(date))
        m,d,y = get_date_values(date)
        if m == target_month and y == target_year:
            print(f"Inserting date at {m} {y}")
            return index + 2
        else:
            continue
    return print("Date not found")
    

def price_expansion(sheet, row):
    sheet.format('I2:I999', {"numberFormat": {"type": "NUMBER"}})
    event, timing, customer, service, id, effective_date, invoice_schedule, invoice_date, invoice_amount, service_term = get_lifecycle_fields(sheet, row)
    print(f"{customer} {service} recogintion schedule")
    schedule = MasterSheet.worksheet(f"{customer} {service} recognition schedule") #Finds recognition schedule based on Customer, Service touple
    if timing == "Current Term": #Checks to see if we edit existing term or modify the following term
        schedule.format('C2:F50', {"numberFormat": {"type": "NUMBER"}})
        print("hello")
        cells = []
        balances = schedule.col_values(6)
        incomes = schedule.col_values(5)
        def_rev_decreases = schedule.col_values(4)
        # We need to find the recognition schedule.
        #Invoice date!
        # Find the date that is closest to Invoice date
        m,d,y = get_date_values(invoice_date) #get values
        target_m, target_y = find_month(m,d,y) #generate targets
        print(target_m, target_y)
        invoice_row = month_lookup(target_m, target_y, schedule) #lookup closest date row
        m,d,y = get_date_values(effective_date)
        # Add the increase to the def - rev increase column. ALWAYS on the invoice date!
        cells.append(Cell(invoice_row,3, f"{invoice_amount}"))
        #Effective date!
        # Find the date that is closest to effective date
        m,d,y = get_date_values(effective_date )#get values
        target_m, target_y = find_month(m,d,y) # generate targets
        print(f"Target month: {target_m}")
        effective_row = month_lookup(target_m, target_y, schedule) #lookup closest date row
        print(f"effective row: {effective_row}")
        monthstogo = 13 - effective_row + 1
        #Now we need to update def rev in that row
        old_def_rev_decrease = float(def_rev_decreases[effective_row-1]) #find original recognition value
        print(old_def_rev_decrease)
        def_rev_decrease = (float(invoice_amount) / (monthstogo * -1)) + old_def_rev_decrease #increase it by new invoice amount / months to go
        print(f"New def rev increase = {def_rev_decrease}")
        # Update def rev decrease column
        for x in range(effective_row, 14):
            cells.append(Cell(x, 4, def_rev_decrease))
        # Update new income number
        income = def_rev_decrease * -1
        for x in range(effective_row, 14):
            cells.append(Cell(x, 5, income))
        # Update the balances
        balance = float(balances[effective_row-2])
        print(f"Current Balance: {balance}")
        for x in range(effective_row, 14):
            print(x)
            if x != invoice_row:
                balance-=income
            else:
                balance = balance - income + float(invoice_amount)
            print(f"Current Balance: {balance}")
            cells.append(Cell(x,6, balance))
            
    
        schedule.update_cells(cells, value_input_option='USER_ENTERED') #batch update the cells
        schedule.format('C2:F50', {"numberFormat": {"type": "CURRENCY"}})
        schedule.format('B2:B50', {"numberFormat": {"type": "DATE"}})
        return
    else:
       customer_tuple = f"{customer} {service}"
       update_recognition_schedule(schedule, effective_date, invoice_amount, service_term, id, customer_tuple)
    return

#event, timing, customer, service, id, effective_date, invoice_schedule, invoice_date, invoice_amount, service_term = get_lifecycle_fields = get_lifecycle_fields(lifecycle, 8)
#print(customer, service)
price_expansion(lifecycle, 8)
#complete(8)
