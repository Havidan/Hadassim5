import csv
from datetime import datetime
from queue import Queue
import os
import _thread
import threading
import pandas as pd

def date_validation(date_str):
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        date_part = dt.strftime("%d-%m-%Y")
        time_part = dt.strftime("%H")
        return (date_part, time_part)
    except ValueError:
        return False

def process_data_frame(df, file_name, result_queue, thread_finishes):
    print(f"Thread started for Parquet file: {file_name}")
    check_duplicate_set = set()
    average_per_hour = {}
    processed_count = 0

    for index, row in df.iterrows():
        processed_count += 1
        date_value = row.iloc[0]
        date_str = str(date_value).strip()
        value_from_row = row.iloc[1]

        if value_from_row is not None:
            try:
                value = float(value_from_row)
            except ValueError:
                print(f"Warning (Parquet - {file_name}): Invalid value format - {value_from_row} at {date_str}")
                continue

            try:
                date_obj = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
                date_part = date_obj.strftime("%d-%m-%Y")
                hour = date_obj.strftime("%H")
                full_date_time = date_obj.strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                print(f"Warning (Parquet - {file_name}): Invalid date format - {date_str}")
                continue

            if full_date_time in check_duplicate_set:
                continue
            check_duplicate_set.add(full_date_time)

            if hour not in average_per_hour:
                average_per_hour[hour] = (value, 1)
            else:
                sum_values, count = average_per_hour[hour]
                average_per_hour[hour] = (sum_values + value, count + 1)
        else:
            print(f"Warning (Parquet - {file_name}): Skipping row with None value at {date_str}")
            continue

    results = []
    for hour, sum_tuple in average_per_hour.items():
        avg_value = sum_tuple[0] / sum_tuple[1]
        formatted_time = f"{hour}:00:00"
        results.append([f"{date_part} {formatted_time}", f"{avg_value:.2f}"])

    print(f"Thread finished for Parquet file: {file_name}. Processed {processed_count} rows, found {len(results)} average values.")
    result_queue.put(results)
    thread_finishes.set()

def process_daily_csv_file(file_path, file_name, result_queue, thread_finishes):
    print(f"Thread started for CSV file: {file_name}")
    check_duplicate_set = set()
    average_per_hour = {}
    processed_count = 0

    try:
        with open(file_path, mode='r', encoding='utf-8-sig') as file:
            csv_reader = csv.reader(file)
            next(csv_reader) # Skip the header row
            for row in csv_reader:
                processed_count += 1
                if not row or len(row) < 2:
                    continue
                date_str = row[0].strip()
                value_str = row[1].strip().lower()

                if not value_str or value_str in ["not_a_number", "nan"]:
                    continue

                try:
                    date_obj = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S" if ".parquet_part.csv" in file_name else "%d/%m/%Y %H:%M")
                    date_part = date_obj.strftime("%d-%m-%Y")
                    hour = date_obj.strftime("%H")
                except ValueError as e:
                    print(f"Warning (CSV - {file_name}): Invalid date format - {date_str} - {e}")
                    continue

                if date_str in check_duplicate_set:
                    continue
                check_duplicate_set.add(date_str)

                try:
                    value = float(row[1])
                except ValueError:
                    print(f"Warning (CSV - {file_name}): Invalid value format - {row[1]} at {date_str}")
                    continue

                if hour not in average_per_hour:
                    average_per_hour[hour] = (value, 1)
                else:
                    sum_values, count = average_per_hour[hour]
                    average_per_hour[hour] = (sum_values + value, count + 1)
    except FileNotFoundError:
        print(f"Error: Could not open file {file_path}")
        result_queue.put([]) # שלח רשימה ריקה כדי לא לפגוע בתוצאות

    results = []
    for hour, sum_tuple in average_per_hour.items():
        if sum_tuple[1] > 0:
            avg_value = sum_tuple[0] / sum_tuple[1]
            formatted_time = f"{hour}:00:00"
            results.append([f"{date_part} {formatted_time}", f"{avg_value:.2f}"])

    print(f"Thread finished for CSV file: {file_name}. Processed {processed_count} rows, found {len(results)} average values.")
    result_queue.put(results)
    thread_finishes.set()
    if ".parquet_part.csv" in file_name:
        try:
            os.remove(file_path)
            print(f"Removed temporary file: {file_path}")
        except OSError as e:
            print(f"Error removing temporary file {file_path}: {e}")

def seperate_by_date(file_path):
    print(f"Starting seperate_by_date for: {file_path}")
    files_by_date = {}
    file_extension = os.path.splitext(file_path)[1].lower()
    print(f"Detected file extension: {file_extension}")

    if file_extension == '.csv':
        with open(file_path, mode='r', encoding='utf-8') as file:
            csvreader = csv.reader(file)
            row_count = 0
            for row in csvreader:
                row_count += 1
                if not row or len(row) < 1:
                    continue
                date_str = row[0].strip()
                date_info = date_validation(date_str)
                if date_info:
                    date_part, time_part = date_info
                    file_path_daily = f'C:\\Users\\This_user\\Desktop\\לימודים\\Hadasim\\part_1\\ex_B\\{date_part}.csv'
                    with open(file_path_daily, mode='a', encoding='utf-8-sig' ,newline='') as daily_file:
                        writer = csv.writer(daily_file)
                        writer.writerow(row + [time_part])
                    files_by_date[date_part] = file_path_daily
            print(f"Processed {row_count} rows from CSV for separation.")
    elif file_extension == '.parquet':
        df = pd.read_parquet(file_path)
        print(f"Read {len(df)} rows from Parquet file for separation.")
        for index, row in df.iterrows():
            if len(row) < 2:
                continue
            date_value = row.iloc[0]
            value = row.iloc[1]
            date_str = str(date_value).strip()
            date_info = date_validation(date_str)
            if date_info:
                date_part, time_part = date_info
                file_path_daily = f'C:\\Users\\This_user\\Desktop\\לימודים\\Hadasim\\part_1\\ex_B\\{date_part}.parquet_part.csv'
                with open(file_path_daily, mode='a', encoding='utf-8-sig' ,newline='') as daily_file:
                    writer = csv.writer(daily_file)
                    if os.stat(file_path_daily).st_size == 0:
                        writer.writerow(["date", "value"]) # כותרות רק אם הקובץ חדש
                    writer.writerow([f"{date_str}", f"{value}"])
                files_by_date[date_part] = file_path_daily # שמור את נתיב הקובץ

        print(f"Separated Parquet file into daily CSV files.")

    print("Starting thread creation...")
    result_queue = Queue()
    threads = []
    thread_count = 0

    for file_path_to_process in files_by_date.values():
        thread_finishes = threading.Event()
        thread_count += 1
        file_name = os.path.basename(file_path_to_process)
        print(f"Creating thread {thread_count} for: {file_name}")
        thread = _thread.start_new_thread(process_daily_csv_file, (file_path_to_process, file_name, result_queue, thread_finishes))
        threads.append(thread_finishes)

    print(f"Created {len(threads)} threads.")
    print("Waiting for threads to finish...")
    for i, thread in enumerate(threads):
        thread.wait()
        print(f"Thread {i+1} finished.")

    print("All threads finished. Writing to final_file.csv")
    with open(f"C:\\Users\\This_user\\Desktop\\לימודים\\Hadasim\\part_1\\ex_B\\final_file1.csv", mode="w", encoding="utf-8-sig", newline="") as final_file:
        csv_writer = csv.writer(final_file)
        csv_writer.writerow(["date", "average value"])
        write_count = 0
        while not result_queue.empty():
            item = result_queue.get()
            for row in item:
                csv_writer.writerow(row)
                write_count += 1
        print(f"Finished writing {write_count} rows to final_file.csv")

file_path = 'C:\\Users\\This_user\\Desktop\\לימודים\\Hadasim\\part_1\\ex_B\\time_series.csv'
seperate_by_date(file_path)

#file_path_parquet = 'C:\\Users\\This_user\\Desktop\\לימודים\\Hadasim\\part_1\\ex_B\\time_series.parquet' # Example parquet file path
#seperate_by_date(file_path_parquet)