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
import sys




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
    row-=1
    sheet.format('I2:I1000', {"numberFormat": {"type": "NUMBER"}})
    sheet.format('F2:F1000', {"numberFormat": {"type": "DATE"}})
    sheet.format('H2:H1000', {"numberFormat": {"type": "DATE"}})
    sheet.format('J2:J1000', {"numberFormat": {"type": "DATE"}})
    length = sheet.col_values(1)
    if (0 < row < len(length)):
        event = length[row]
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
    else:
        raise IndexError(f"Row {row+1} is out of range.")

#Get customer service touple from lifecycle sheet
# Find line to start
# Build out next recognition schedule.
def update_recognition_schedule(schedule, effective_date, invoice_amount, service_term, customer_tuple):
    col_check = schedule.col_values(1) #grab list of column values. 
    line_start = len(col_check) + 1 #logically the last column value is one less than our desired row.
    print(f"Line start: {line_start}")
    cells = []
    #add new dates
    date_time = datetime.strptime(effective_date, "%m/%d/%Y")
    current_date = date_time
    for x in range(line_start,(line_start+int(service_term))):
        print(f"Current line{x}")
        current_date_str = current_date.strftime("%#m/%#d/%Y")
        cells.append(Cell(x,2, current_date_str))
        current_date += relativedelta(months=1)
    #add customers
    for x in range(line_start, line_start+int(service_term)):
        cells.append(Cell(x,1, customer_tuple))
    # def rev increase
    cells.append(Cell(line_start,3, invoice_amount))
    # def rev decrease
    print(invoice_amount)
    def_rev_decrease = float(invoice_amount) / float(service_term) * float(-1)
    print(f"Float amount = {def_rev_decrease}")
    for x in range(line_start, line_start+int(service_term)):
        cells.append(Cell(x, 4, def_rev_decrease ))
    # income
    income = def_rev_decrease * float(-1)
    for x in range(line_start, line_start+int(service_term)):
        cells.append(Cell(x, 5, income))
    # balance 
    balance = float(invoice_amount) - income
    cells.append(Cell(line_start, 6, balance))
    for x in range(line_start+1, line_start+int(service_term)):
        balance -= income
        cells.append(Cell(x,6,balance))


    
    schedule.update_cells(cells, value_input_option='USER_ENTERED') #batch update the cells
    schedule.format('C2:F100', {"numberFormat": {"type": "CURRENCY"}})
    schedule.format('B2:B100', {"numberFormat": {"type": "DATE"}})
    return print("finished")

if __name__ == "__main__":
    try:
        event, timing, customer, service, id, effective_date, invoice_schedule, invoice_date, invoice_amount, service_term = get_lifecycle_fields(lifecycle,1)
    except IndexError as e:
        print(e)
        sys.exit(1)
    # Find associated recognition schedule
    customer_tuple = f"{customer} {service}"
    title = f"{customer} {service} recognition schedule"
    print(title)
    #schedule = MasterSheet.worksheet(title)
    # update_recognition_schedule(schedule, effective_date, invoice_amount, service_term, id, customer_tuple)
    # complete(10)