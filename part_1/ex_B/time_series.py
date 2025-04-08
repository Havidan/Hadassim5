import csv
from datetime import datetime
from queue import Queue
import os
import _thread
import threading  
import pandas as pd

def date_validation(date_str):
    try:
        dt = datetime.strptime(date_str, "%d/%m/%Y %H:%M")
        date_part = dt.strftime("%d-%m-%Y")  
        time_part = dt.strftime("%H")
        return (date_part, time_part)
    except ValueError:
        return False

def process_daily_file(file,file_name, result_queue,thread_finishes):
    
    check_duplicate_set = set()
    average_per_hour = {}
    csv_reader = csv.reader(file)
    
    for row in csv_reader:
        date_str = row[0]  

        if not row[1].strip() or row[1].strip().lower() in ["not_a_number", "nan"]:  
            continue  
    
        try:
            date_obj = datetime.strptime(date_str, "%d/%m/%Y %H:%M")
            date_part = date_obj.strftime("%d-%m-%Y")  
            hour = date_obj.strftime("%H")  
        except ValueError:
            continue  
        
        if date_str in check_duplicate_set:
            continue
        check_duplicate_set.add(date_str)

        try:
            value = float(row[1]) 
        except ValueError:
            continue  

        if hour not in average_per_hour:
            average_per_hour[hour] = (value, 1)  
        else:
            sum_values, count = average_per_hour[hour]
            average_per_hour[hour] = (sum_values + value, count + 1)  

    results = []
    for hour, sum_tuple in average_per_hour.items():
        avg_value = sum_tuple[0] / sum_tuple[1]
        formatted_time = f"{hour}:00:00"  
        results.append([f"{date_part} {formatted_time}", f"{avg_value:.2f}"])  
    file.close()
    os.remove(f"C:\\Users\\This_user\\Desktop\\לימודים\\Hadasim\\part_1\\ex_B\\{file_name}.csv")  
    result_queue.put(results)
    thread_finishes.set() 

def seperate_by_date(file_path):
    
    with open(file_path, mode='r', encoding='utf-8') as file:

        csvreader = csv.reader(file)
        files_by_dats={}
        for row in csvreader:
            date=date_validation(row[0])
            if date:
                if date[0] not in files_by_dats:
                    files_by_dats[date[0]] = open(f'C:\\Users\\This_user\\Desktop\\לימודים\\Hadasim\\part_1\\ex_B\\{date[0]}.csv', mode='+a',  encoding='utf-8-sig' ,newline='')
                writer = csv.writer(files_by_dats[date[0]])
                row.append(date[1])
                writer.writerow(row)
        result_queue = Queue()
        threads = []
        for file_name, file in files_by_dats.items():
            file.seek(0) 
            thread_finishes = threading.Event()
            thread = _thread.start_new_thread(process_daily_file, (file, file_name,result_queue, thread_finishes))
            threads.append(thread_finishes) 

        for thread in threads:
                thread.wait()  


        with open(f"C:\\Users\\This_user\\Desktop\\לימודים\\Hadasim\\part_1\\ex_B\\final_file.csv", mode="w", encoding="utf-8-sig", newline="") as final_file:
            csv_writer = csv.writer(final_file)
            csv_writer.writerow(["date", "average value"])
            while not result_queue.empty():
                item = result_queue.get()  
                for row in item:
                    csv_writer.writerow(row)  

file_path = 'C:\\Users\\This_user\\Desktop\\לימודים\\Hadasim\\part_1\\ex_B\\time_series.csv'
seperate_by_date(file_path)
