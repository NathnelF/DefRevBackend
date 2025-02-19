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
from renewal import get_lifecycle_fields



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

def convert_currency_string_to_int(currency_str):
    # Remove the dollar sign
    currency_str = currency_str.replace('$', '')
    # Remove the commas
    currency_str = currency_str.replace(',', '')
    # Convert the string to a float to handle the decimal part
    currency_float = float(currency_str)
    # Convert the float to an integer
    currency_int = int(currency_float)
    return currency_int

def cell_exists(worksheet, value):
    all_values = worksheet.get_all_values()
    for row in all_values:
        if value in row:
            return True
    return False

def column_lookup(worksheet, val):
    try:
        cell = worksheet.find(val)
        track_read_request()
        return cell.col
    except AttributeError:
        print(f"Value '{val}' not found in sheet '{worksheet}'")

def row_lookup(worksheet, val):
    try:
        cell = worksheet.find(val)
        track_read_request()
        return cell.row
    except AttributeError:
        print(f"Value '{val}' not found in sheet '{worksheet}'")


def get_recognition_schedule_values(id):
    contracts = MasterSheet.worksheet("Contracts")
    track_read_request()
    row = row_lookup(contracts, id)
    type = contracts.acell(f'I{row}').value
    track_read_request()
    if type == "Annual" or type == "annual":
        def_rev = contracts.acell(f'K{row}').value
        track_read_request()
        months = contracts.acell(f'F{row}').value
        track_read_request()
        schedule = contracts.acell(f'N{row}').value
        track_read_request()
        end_date = contracts.acell(f'H{row}').value
        track_read_request()
        start_date = contracts.acell(f'G{row}').value
        track_read_request()
        churn_date = contracts.acell(f'J{row}').value
        track_read_request()
        price_increase = contracts.acell(f'M{row}').value
        track_read_request()
        
        #print("Returning:", start_date, def_rev, months, schedule)
        return start_date, end_date, churn_date, def_rev, months, schedule, price_increase
    else:
        return "This is a monthly contract"
    
def check_schedule_status(id):
    contracts = MasterSheet.worksheet("Contracts")
    track_read_request()
    row = row_lookup(contracts, id)
    schedule = contracts.acell(f'N{row}').value
    track_read_request()
    if schedule: 
        return True
    else:
        return False
    
def update_schedule_status(id, url):
    contracts = MasterSheet.worksheet("Contracts")
    track_read_request()
    contracts_row = row_lookup(contracts, id)
    contracts.update_acell(f"N{contracts_row}", url)
    return print("Schedule status updated")

def split_date(date):
    date = date.split("/")
    month = int(date[0])
    year = date[2]
    return month, year

def churn_contract(schedule, churn_date):
    dates = schedule.col_values(2)
    numeric, year = split_date(churn_date)
    for index, date in enumerate(dates):
        m,y = split_date(date)
        if (m == numeric and y == year):
            row = index + 2
            #update row to be churned.
            #clear values under it.
    return

def increment_date(date, timeframe, value):
    valid_timeframes = {"years", "months", "weeks", "days", "hours", "minutes", "seconds"}
    if timeframe not in valid_timeframes:
        raise ValueError(f"Invalid timeframe: '{timeframe}'. Must be one of {valid_timeframes}.")
    date = datetime.strptime(date, "%m/%d/%Y")
    date += relativedelta(**{timeframe: value})
    date = date.strftime("%#m/%#d/%Y")
    return date

def create_recognition_schedule(customer, service, title_list, id):
    schedule_title = f"{customer} {service} recognition schedule"
    
    # Check if the worksheet already exists
    worksheets = MasterSheet.worksheets()
    for sheet in worksheets:
        if sheet.title == schedule_title:
            print(f"Schedule '{schedule_title}' already exists. Doing nothing.")
            return  # If it exists, do nothing and return

    # Establish cell array to be updated later.
    # Initalize cel array with static customer service touple column
    cells = [
        Cell(1, 1, "Customer"),
        Cell(1, 2, "Date")
    ]
    #get important values from contract sheet
    start_date, end_date, churn_date, def_rev, months, schedule, price_increase = get_recognition_schedule_values(id)
    #This is how much def rev increases by initially
    def_rev_increase = convert_currency_string_to_int(def_rev)
    print(months)
    months = int(months)
    if (months == 36):
        def_rev_increase = def_rev_increase * 3
    #This will be the monthly income based on revenue recognition
    income = def_rev_increase / months
    print(f"income will be {income}")
    #This is the monthly deferred revenue decrease
    def_rev_decrease = income * -1
    schedule = MasterSheet.add_worksheet(title=f"{customer} {service} recognition schedule", rows=500, cols=20)
    url = f"{master_url}/edit?gid={schedule.id}#gid={schedule.id}"
    
    for x in range(2, (months+2)):
        cells.append(Cell(x,1, f"{customer} {service}"))
    cells.append(Cell(2,2, f"{start_date}"))
    #date_time = datetime.strptime(date, "%m/%d/%Y")
    current_date = datetime.strptime(start_date, "%m/%d/%Y")
    for x in range(3,(months+2)):
        current_date += relativedelta(months=1)
        current_date_str = current_date.strftime("%#m/%#d/%Y")
        cells.append(Cell(x,2, current_date_str))
    final_date = current_date
    print(f'final date is {final_date}')
    next_start_date_dt = final_date + relativedelta(months=1)
    next_end_date_dt = next_start_date_dt + relativedelta(years=1)
    next_start_date = next_start_date_dt.strftime("%#m/%#d/%Y")
    next_end_date = next_end_date_dt.strftime("%#m/%#d/%Y")
    print(f"Start of next term is {next_start_date}")
    print(f"End of next term is {next_end_date}")
    #Def rev increase tracking
    cells.append(Cell(1,3, "Def Rev Increase"))
    cells.append(Cell(2,3, f"{def_rev_increase}"))
    #Def rev decrease tracking
    cells.append(Cell(1,4, "Def Rev Decrease"))
    for x in range (2,(months+2)):
        cells.append(Cell(x, 4, f"{def_rev_decrease}"))
    #Income tracking
    cells.append(Cell(1,5, "Income"))
    for x in range (2,(months+2)):
        cells.append(Cell(x, 5, f"{income}"))
    #Balance tracking
    cells.append(Cell(1,6, "Balance"))
    balance = def_rev_increase - income
    cells.append(Cell(2,6, f"{balance}")) #Initial balance
    for x in range(3,(months+2)):
        balance -= income #each month balance changes negatively by income
        cells.append(Cell(x,6, f"{balance}"))
    

    update_schedule_status(id, url)
    schedule.update_cells(cells, value_input_option='USER_ENTERED') #batch update the cells
    schedule.format('C2:F50', {"numberFormat": {"type": "CURRENCY"}})
    schedule.format('B2:B50', {"numberFormat": {"type": "DATE"}})
    new_amount = float(def_rev_increase)
    start_m, start_y = split_date(next_start_date)
    end_m, end_y = split_date(end_date)
    print(f"start month: {start_m}, end month {end_m}, start year: {start_y}, end year: {end_y}")
    end_date_dt = datetime.strptime(end_date, "%m/%d/%Y")
    print(end_date_dt)
    while True:
        if start_m == end_m and start_y == end_y:
            break
        print(f"Next start date: {next_start_date_dt}, end date: {end_date_dt}. Difference = {(abs(next_start_date_dt - end_date_dt))}")
        if abs(next_start_date_dt - end_date_dt).days == 1:
            break
        if price_increase:
            percent = (int(price_increase)/100) + 1
            print(f"Percent is :{percent}")
            new_amount = int(new_amount * percent)
            print(f"New amount is: {new_amount}")
        else:
            new_amount = def_rev_increase
        customer_tuple = f"{customer} {service}"
        print(f"Lifecycle values: {new_amount}, {months}, {customer}, {service}")
        update_recognition_schedule(schedule, next_start_date, new_amount, months, customer_tuple)
        #update contracts dates // COMMENTED OUT FOR NOW
        # contract_row = row_lookup(contracts, id)
        # contracts.update_acell(f"G{contract_row}", f"{next_start_date}")
        next_start_date_dt = datetime.strptime(next_start_date, "%m/%d/%Y")
        next_start_date_dt += relativedelta(years=1)
        print(f"Next start date: {next_start_date}")
        next_start_date = next_start_date_dt.strftime("%#m/%#d/%Y")
        next_end_date = datetime.strptime(next_end_date, "%m/%d/%Y")
        next_end_date += relativedelta(years=1)
        print(f"Next end date: {next_end_date}")
        next_end_date = next_end_date.strftime("%#m/%#d/%Y")
        print(f"Original end date: {end_date}")
        start_m, start_y = split_date(next_start_date)
        print(f"New start month: {start_m}, new start year: {start_y}")
            # check for churn
    return


def run_recognition_schedules(worksheet, title_list):
    global read_request_counter
    ids = worksheet.col_values(3)  # get all Id
    track_read_request()
    print(ids)
    customers = worksheet.col_values(1) #get all customer names
    track_read_request()
    services = worksheet.col_values(2)
    track_read_request()
    print(customers)
    index = 0
    for id in ids:
        while read_request_counter >= 125:
                print("Read request limit reached. Waiting for 60 seconds...")
                time.sleep(70)
                read_request_counter = 0  # Reset counter after waiting
        customer = customers[index]
        service = services[index]
        print(customer)
        print(id)
        if check_schedule_status(id) == True:
            print("Schedule already exists")
        else:
            #list_number = get_schedule_number(title_list, customer)
            create_recognition_schedule(customer, service, title_list, id)
            title_list = get_sheet_list_titles(MasterSheet.worksheets())
        index+=1
    
    return print("All done")


if __name__ == "__main__":
    sheet_list = MasterSheet.worksheets()
    sheet_list_titles = get_sheet_list_titles(sheet_list)
    contracts = MasterSheet.worksheet("Contracts")
    #create_recognition_schedule("Akumin", "NWG Manage", sheet_list_titles, "fe1")
    #create_recognition_schedule("Production Saw & Machine", "MSS-FW", sheet_list_titles, "fe45")
    run_recognition_schedules(contracts, sheet_list_titles)
    #print(date)

    # start_date = "6/1/2025"
    # end_date = "5/31/2025"

    # start_dt = datetime.strptime(start_date, "%m/%d/%Y")
    # end_dt = datetime.strptime(end_date, "%m/%d/%Y")

    # dif = abs(start_dt - end_dt)
    # print(dif.days)

