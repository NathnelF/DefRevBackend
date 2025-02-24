import gspread
from google.oauth2.service_account import Credentials
from gspread.exceptions import GSpreadException
from gspread.exceptions import APIError
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from gspread import Cell
import logging




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

def complete(row):
    cells = []
    cells.append(Cell(row, 14, "completed"))
    lifecycle.update_cells(cells, value_input_option='USER_ENTERED') #batch update the cells
    return