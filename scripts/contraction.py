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
import argparse
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

#Get customer service touple from lifecycle sheet
# Find line to start
# Build out next recognition schedule.
# def update_recognition_schedule(schedule, effective_date, invoice_amount, service_term, id, customer_tuple):
#     line_start = int(service_term) + 2
#     cells = []
#     #add new dates
#     date_time = datetime.strptime(effective_date, "%m/%d/%Y")
#     current_date = date_time
#     for x in range(line_start,(line_start*2 - 2)):
#         current_date_str = current_date.strftime("%#m/%#d/%Y")
#         cells.append(Cell(x,2, current_date_str))
#         current_date += relativedelta(months=1)
#     #add customers
#     for x in range(line_start, line_start*2 -2):
#         cells.append(Cell(x,1, customer_tuple))
#     # def rev increase
#     cells.append(Cell(line_start,3, invoice_amount))
#     # def rev decrease
#     def_rev_decrease = int(invoice_amount) / int(service_term) * - 1
#     for x in range(line_start, line_start * 2 - 2):
#         cells.append(Cell(x, 4, def_rev_decrease ))
#     # income
#     income = def_rev_decrease * -1
#     for x in range(line_start, line_start * 2 - 2):
#         cells.append(Cell(x, 5, income))
#     # balance 
#     balance = int(invoice_amount) - income
#     cells.append(Cell(line_start, 6, balance))
#     for x in range(line_start+1, line_start * 2 - 2):
#         balance -= income
#         cells.append(Cell(x,6,balance))


    
    # schedule.update_cells(cells, value_input_option='USER_ENTERED') #batch update the cells
    # schedule.format('C2:F100', {"numberFormat": {"type": "CURRENCY"}})
    # schedule.format('B2:B100', {"numberFormat": {"type": "DATE"}})
    # return print("finsihed")

def find_min_index(target_index, list):
    closest_index = None
    min_distance = float('inf')
    for i,elem in enumerate(list):
        if elem:
            distance = abs(i - target_index)
            if distance < min_distance:
                min_distance = distance
                closest_index = i
    return closest_index


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CL argument parser")
    parser.add_argument("Row", type=int, help="Input the row of the lifecycle event you want to run")
    args = parser.parse_args()
    try:
        event, timing, customer, service, id, effective_date, invoice_schedule, invoice_date, invoice_amount, service_term = get_lifecycle_fields(lifecycle,args.Row)
    except IndexError as e:
        print(e)
        sys.exit(1)
    _, _, _, _, _, _, price_increase = get_recognition_schedule_values(id)
    # Find associated recognition schedule
    customer_tuple = f"{customer} {service}"
    title = f"{customer} {service} recognition schedule"
    print(title)
    schedule = MasterSheet.worksheet(title)
    target_m, target_y = split_date(effective_date)
    schedule_dates = schedule.col_values(2)[1:]
    retroactive = False
    increases = schedule.col_values(3)[1:]
    print(increases)
    print(len(schedule_dates))   
    for i,date in enumerate(schedule_dates):
        m,y = split_date(date)
        if target_m == m and target_y == y:
            print(date)
            retroactive = True
            target_index = i
            print(target_index)
    if retroactive == True:
        print("We will have to insert the contraction into the schedule.")
        end_of_sched = find_min_index(target_index, increases)
        print(f"The last row in current schedule is {end_of_sched}")
        print(increases[end_of_sched])
        clear_point = end_of_sched + 2
        end_point = len(schedule_dates) + 1
        range = [f"A{clear_point}:F{end_point}"]
        next_date = schedule_dates[end_of_sched]
        print(f"Next date: {next_date}")
        schedule.batch_clear(range)
        final_date = schedule_dates[end_point - 2]
        final_date = increment_date(final_date, "months", 1)
        print(f"Final date: {final_date}")
        start_m, start_y = split_date(next_date)
        end_m, end_y = split_date(final_date)
        print(f"start month: {start_m}, end month {end_m}, start year: {start_y}, end year: {end_y}")
        while True:
            if (start_m == end_m and start_y == end_y):
                break
            update_recognition_schedule(schedule, next_date, invoice_amount, service_term, customer_tuple)
            next_date = increment_date(next_date, "years", 1)
            start_m, start_y = split_date(next_date)
            if price_increase:
                percent = (float(price_increase) / float(100)) + float(1)
                new_amount = int(float(invoice_amount) * percent)
                invoice_amount = new_amount
            


        #clear everything under the current schedule.
        #run the repeated update cycle for remaining terms using new amount.
        #apply price increase if necessary.
    #update_recognition_schedule(schedule, effective_date, invoice_amount, service_term, customer_tuple)
    #complete(11)