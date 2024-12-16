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
scheduled_list = []

def get_schedule_number(title_list, customer):
    list = []
    for title in title_list:
        print(title)
        print(customer)
        if customer in title:
            list.append(title)
    print(f"This is the list: {list}")
    if len(list) > 0:
        print(f"List length is {len(list)}")
        last_entry = list[len(list)-1].split(' ')
        print(f"Last Entry is {last_entry}")
        title_number = int(last_entry[len(last_entry)-1]) + 1
        print(f"Recognition schedule number: {title_number}")
    else:
        print("First instance of customer")
        title_number = 1
    return title_number


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

def get_recognition_schedule_values(id):
    contracts = MasterSheet.worksheet("Contracts")
    track_read_request()
    row = row_lookup(contracts, id)
    type = contracts.acell(f'H{row}').value
    track_read_request()
    if type == "Annual" or type == "annual":
        date = contracts.acell(f'E{row}').value
        track_read_request()
        def_rev = contracts.acell(f'J{row}').value
        track_read_request()
        months = 12
        schedule = contracts.acell(f'K{row}').value
        track_read_request()

        return date, def_rev, months, schedule
    else:
        return "This is a monthly contract"
    
def check_schedule_status(id):
    contracts = MasterSheet.worksheet("Contracts")
    track_read_request()
    row = row_lookup(contracts, id)
    schedule = contracts.acell(f'K{row}').value
    track_read_request()
    if schedule: 
        return True
    else:
        return False

def update_schedule_status(id, url):
    contracts = MasterSheet.worksheet("Contracts")
    track_read_request()
    contracts_row = row_lookup(contracts, id)
    contracts.update_acell(f"K{contracts_row}", url)
    return print("Schedule status updated")


def create_recognition_schedule(customer, service, title_list, id):
    schedule_title = f"{customer} {service} recognition schedule"
    
    # Check if the worksheet already exists
    worksheets = MasterSheet.worksheets()
    for sheet in worksheets:
        if sheet.title == schedule_title:
            print(f"Schedule '{schedule_title}' already exists. Doing nothing.")
            return  # If it exists, do nothing and return

    cells = [
        Cell(1, 1, "Customer"),
        Cell(2,1, f"{customer} {service}"),
        Cell(3,1, f"{customer} {service}"),
        Cell(4,1, f"{customer} {service}"),
        Cell(5,1, f"{customer} {service}"),
        Cell(6,1, f"{customer} {service}"),
        Cell(7,1, f"{customer} {service}"),
        Cell(8,1, f"{customer} {service}"),
        Cell(9,1, f"{customer} {service}"),
        Cell(10,1, f"{customer} {service}"),
        Cell(11,1, f"{customer} {service}"),
        Cell(12,1, f"{customer} {service}"),
        Cell(13,1, f"{customer} {service}"),
        Cell(14,1, f"{customer} {service}"),
        Cell(1, 2, "Date")
    ]
    date, def_rev, months, schedule = get_recognition_schedule_values(id)
    def_rev = convert_currency_string_to_int(def_rev)
    months = int(months)
    income = def_rev / months
    schedule = MasterSheet.add_worksheet(title=f"{customer} {service} recognition schedule", rows=100, cols=20)
    url = f"{master_url}/edit?gid={schedule.id}#gid={schedule.id}"
    update_schedule_status(id, url)
    # schedule.update_cell(1,1, f"{customer}")
    # schedule.update_cell(1,2, "Date")
    # schedule.update_cell(2,2, f"{date}")
    cells.append(Cell(2,2, f"{date}"))
    date_time = datetime.strptime(date, "%m/%d/%Y")
    current_date = date_time
    for x in range(3,15):
        current_date += relativedelta(months=1)
        current_date_str = current_date.strftime("%#m/%#d/%Y")
        #schedule.update_cell(x,2, current_date_str)
        cells.append(Cell(x,2, current_date_str))
    #schedule.update_cell(1,3, "Deferred Revenue")
    cells.append(Cell(1,3, "Deferred Revenue"))
    #schedule.update_cell(2,3, f"{def_rev}")
    cells.append(Cell(2,3, f"{def_rev}"))
    for x in range (3,15):
        #schedule.update_cell(x, 3, f"-{income}")
        cells.append(Cell(x, 3, f"-{income}")) #this is actually deferred revenue monthly change (NOT INCOME)
    #schedule.update_cell(1,4, "Income")
    cells.append(Cell(1,4, "Income"))
    #schedule.update_cell(2,4, "0")
    cells.append(Cell(2,4, "0")) #first month income is always 0
    for x in range (3,15):
        #schedule.update_cell(x, 4, f"{income}")
        cells.append(Cell(x, 4, f"{income}")) #monthly income change
    #schedule.update_cell(1,5, "Balance")
    cells.append(Cell(1,5, "Balance"))
    balance = def_rev
    #schedule.update_cell(2,5, f"{balance}")
    cells.append(Cell(2,5, f"{balance}")) #Initial balance
    for x in range(3,15):
        balance -= income #each month balance changes negatively by income
        #schedule.update_cell(x,5, f"{balance}")
        cells.append(Cell(x,5, f"{balance}"))
    #schedule.update_cell(1,6, "Monthly Change")
    cells.append(Cell(1,6, "Monthly Change"))

    schedule.update_cells(cells, value_input_option='USER_ENTERED') #batch update the cells
    schedule.format('C2:E14', {"numberFormat": {"type": "CURRENCY"}})
    schedule.format('B2:B14', {"numberFormat": {"type": "DATE"}})
    return 

def customer_name(title):
    contracts = MasterSheet.worksheet("Contracts")
    track_read_request()
    customers = contracts.col_values(1)[1:]
    track_read_request()
    print(customers)
    for customer in customers:
        print(customer)
        if customer in title:
            print(f"{customer} matched successfully")
            return customer
    return "No name"

def get_day(worksheet):
    date = worksheet.acell('B2').value
    track_read_request()
    date_time = datetime.strptime(date, "%m/%d/%Y")
    day = date_time.day
    return day

def date_to_dateTime(day, month, year):
    date_str = f"{day} {month} {year}"
    date_time = datetime.strptime(date_str, "%d %B %Y")
    return date_time

def dateTime_to_date(date_time):
    date = date_time.strftime("%#m/%#d/%Y")
    return date

def find_monthly_defrev(month, year, worksheet):
    #finds the proper day to be searched for
    day = get_day(worksheet)
    #takes the proper day, and given month / year and converts it to the date format that will be found on the recognition schedule
    date_toFind = dateTime_to_date(date_to_dateTime(day, month, year))
    #uses that date to find corresponding row
    
    if cell_exists(worksheet, date_toFind) == True:
        row = worksheet.find(date_toFind).row
        track_read_request()
        #gets balance which is always in E column from proper row
        def_rev = worksheet.acell(f"C{row}").value
        track_read_request()
        def_rev = convert_currency_string_to_int(def_rev)
        return def_rev
    else:
        return 0

def prev_month(month):
    current = date_to_dateTime(1, month, 2001)
    prev = current - relativedelta(months=1)
    prev_month = prev.strftime("%B")
    return prev_month

def prev_total(month, worksheet):
    if month == "January":
        return 0
    else:
        prev = prev_month(month)
        row = worksheet.find(f"{prev} Balance").row
        track_read_request()
        col = worksheet.find(f"{prev} Balance").col
        track_read_request()
        prev_balance = worksheet.cell(row,col+1).value
        track_read_request()
        print(prev_balance)
        prev_total = convert_currency_string_to_int(prev_balance)
        return prev_total

def monthly_report(month, year, report_sheet, titles_list, row_index, col_index):
    global read_request_counter
    #maybe make all the reports go on one sheet together?
    def_rev_total = 0
    cells = [Cell(row_index, col_index, month),Cell(row_index, col_index+1, "Deferred Revenue")] #create headers for report
    for title in titles_list:
        if "schedule" in title.lower():
            print(title)
            while read_request_counter >= 40:
                print("Read request limit reached. Waiting for 60 seconds...")
                time.sleep(75)
                read_request_counter = 0  # Reset counter after waiting
            worksheet = MasterSheet.worksheet(title)
            track_read_request()
            def_rev = find_monthly_defrev(month, year, worksheet)
            def_rev_total += def_rev
            name = customer_name(title)
            cells.append(Cell(row_index+1, col_index, name)) #company
            cells.append(Cell(row_index+1, col_index+1, def_rev)) #monthly def rev for that company
            report_sheet.update_cells(cells, value_input_option='USER_ENTERED') #update sheet
            row_index+=1 #increment row index so next company can go there

    cells = [(Cell(row_index+1, col_index, "Total"))] #create total header
    cells.append(Cell(row_index+1, col_index+1, def_rev_total)) #total amount
    cells.append(Cell(row_index+2,col_index, "Previous total")) #prev amount header
    prev_balance = prev_total(month, report_sheet) #calculate prev_balnce
    cells.append(Cell(row_index+2,col_index+1, prev_balance))  #prev balance 
    cells.append(Cell(row_index+3, col_index, f"{month} Balance")) #ending balance header
    cells.append(Cell(row_index+3, col_index+1, def_rev_total + prev_balance)) #ending balance

    report_sheet.update_cells(cells, value_input_option='USER_ENTERED')
    return

def run_monthy_report(titles_list, year):
    report = MasterSheet.add_worksheet(title=f"Monthly Reports, {year}", rows=100, cols=20)
    counter = 0
    for month in months:
        if counter <= 4:
            print(counter)
            if counter == 0:
                print("Ayo")
                col_index = 1
            print(col_index)
            row_index = 1
            monthly_report(month, year, report, titles_list, row_index, col_index)
            col_index +=3
            counter+=1
        elif counter <= 9:
            print(counter)
            if counter == 5:
                print("Ayo")
                col_index = 1 # reset col index when we jump rows
            print(col_index)
            row_index = 10
            monthly_report(month, year, report, titles_list, row_index, col_index)
            col_index +=3
            counter+=1
        else:
            print(counter)
            if counter == 10:
                print("Ayo")
                col_index = 1
            print(col_index)
            row_index = 19
            monthly_report(month, year, report, titles_list, row_index, col_index)
            col_index +=3
            counter+=1
    report.format('A2:N30', {"numberFormat": {"type": "CURRENCY"}})
    return


def find_monthly_balances(months, year, worksheet):
    # Read the entire column of dates and balances in one go
    dates = worksheet.col_values(2)  # Assuming dates are in column A
    track_read_request()
    balances = worksheet.col_values(5)  # Assuming balances are in column E
    track_read_request()
    
    monthly_balances = []
    for month in months:
        # Find the proper day to be searched for
        day = get_day(worksheet)
        # Convert to the date format that will be found on the recognition schedule
        date_to_find = dateTime_to_date(date_to_dateTime(day, month, year))
        
        try:
            # Find the corresponding row
            row_index = dates.index(date_to_find)
            # Get the balance from the corresponding row
            balance = balances[row_index]
            balance = convert_currency_string_to_int(balance)
        except ValueError:
            # If the date is not found, handle the exception
            balance = 0  # or some default value
        
        monthly_balances.append(balance)
    
    return monthly_balances

def find_monthly_balance(month, year, worksheet):
    #finds the proper day to be searched for
    day = get_day(worksheet)
    #takes the proper day, and given month / year and converts it to the date format that will be found on the recognition schedule
    date_toFind = dateTime_to_date(date_to_dateTime(day, month, year))
    #uses that date to find corresponding row
    row = worksheet.find(date_toFind).row
    track_read_request()
    #gets balance which is always in E column from proper row
    balance = worksheet.acell(f"E{row}").value
    track_read_request()
    balance = convert_currency_string_to_int(balance)
    return balance
            
def amalgamate_data(title_list):
    all_data = pd.DataFrame()
    for title in title_list:
        if "schedule" in title.lower():
            sheet = MasterSheet.worksheet(title)
            data = sheet.get_all_records()
            df = pd.DataFrame(data)
            all_data = pd.concat([all_data, df])
            all_data['Date'] = pd.to_datetime(all_data['Date'])  # Set data in 'Date' column to datetime
            all_data.sort_values(by='Date', ascending=True, inplace=True) 
            all_data['Date'] = all_data['Date'].dt.strftime("%#m/%#d/%Y")

    try:
        new_worksheet = MasterSheet.worksheet("Amalgamated Data")
    except gspread.exceptions.WorksheetNotFound:
        new_worksheet = MasterSheet.add_worksheet(title="Amalgamated Data", rows="1000", cols="20")
    new_worksheet.clear()
    new_worksheet.update([all_data.columns.values.tolist()] + all_data.values.tolist())
    rows = len(new_worksheet.col_values(5))
    cells = []
    for row in range(3, rows):
        formula = f"=E{row-1} + C{row}"
        cells.append(Cell(row,5, formula))

    new_worksheet.update_cells(cells, value_input_option='USER_ENTERED')
    new_worksheet.format(f'E2:E{rows}', {"numberFormat": {"type": "CURRENCY"}, "horizontalAlignment": "LEFT"})


    return print("All done!!")

def month_to_number(month):
    numeric = 0
    if month == "January" or month == "january":
        numeric = 1
    elif month == "February" or month == "february":
        numeric = 2
    elif month == "March" or month == "march":
        numeric = 3
    elif month == "April" or month == "april":
        month = 4
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
        return print("This month is not real")
    return numeric

def split_date(date):
    date = date.split("/")
    month = int(date[0])
    year = date[2]
    return month, year


def new_monthly_report(month, year):
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

def create_balance_sheet(title_list, year):
    global read_request_counter
    
    col_index = 2
    balance_sheet = MasterSheet.add_worksheet(title=f"Balances Sheet {year}", rows=100, cols=20)
    
    cells = [
        Cell(1, 1, "Monthly Balances"),
        Cell(2, 1, "Date")
    ]
    
    # Add months to the first column
    month_row = 3
    for month in months:
        cells.append(Cell(month_row, 1, month))
        month_row += 1
    
    # Add customer balances
    for title in title_list:
        if "schedule" in title.lower():
            while read_request_counter >= 45:
                print("Read request limit reached. Waiting for 60 seconds...")
                time.sleep(65)
                read_request_counter = 0  # Reset counter after waiting
            try:
                title_sheet = MasterSheet.worksheet(title)
                track_read_request()
                balances = find_monthly_balances(months, year, title_sheet)
                
                name = customer_name(title)
                cells.append(Cell(2, col_index, name))  # Customer name header
                for x in range(3, 15):
                    val = balances[x-3]  # Using the pre-fetched balances
                    cells.append(Cell(x, col_index, val))
                col_index += 1
            except gspread.exceptions.APIError as e:
                print(f"Error accessing worksheet '{title}': {e}")
                continue
            

    # Add total balance column
    cells.append(Cell(2, col_index, "Total balance"))
    for x in range(3, 15):
        total_formula = f"=SUM(B{x}:{gspread.utils.rowcol_to_a1(x, col_index-1)})"
        cells.append(Cell(x, col_index, total_formula))
    
    # Batch update the cells
    balance_sheet.update_cells(cells, value_input_option='USER_ENTERED')
    
    #format to currency
    print(col_index)
    currency_range = gspread.utils.rowcol_to_a1(3, 2) + ':' + gspread.utils.rowcol_to_a1(14, col_index)
    print(currency_range)
    balance_sheet.format(currency_range, {"numberFormat": {"type": "CURRENCY"}})

    print("All Done")
    return

def run_recognition_schedules(worksheet, title_list):
    global read_request_counter
    ids = worksheet.col_values(3)  # get all Id
    track_read_request()
    print(ids)
    customers = worksheet.col_values(1) #get all customer names
    services = worksheet.col_values(2)
    track_read_request()
    print(customers)
    index = 0
    for id in ids:
        while read_request_counter >= 140:
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
 


    
sheet_list = MasterSheet.worksheets()
sheet_list_titles = get_sheet_list_titles(sheet_list)
contracts = MasterSheet.worksheet("Contracts")


#run_recognition_schedules(contracts, sheet_list_titles)
#time.sleep(60)
#sheet_list = MasterSheet.worksheets()
#sheet_list_titles = get_sheet_list_titles(sheet_list)
#amalgamate_data(sheet_list_titles)
new_monthly_report("March", "2024")

#time.sleep(60)
#run_monthy_report(sheet_list_titles, "2024")
#time.sleep(60)
#create_balance_sheet(sheet_list_titles, '2024')


