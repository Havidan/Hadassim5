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
        print(f"Parquet - Date value: {date_value}, Type: {type(date_value)}")
        date_str = str(date_value).strip()
        print(f"Parquet - Date string (stripped): {date_str}")
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

def process_daily_csv_file(file, file_name, result_queue, thread_finishes):
    print(f"Thread started for CSV file: {file_name}")
    check_duplicate_set = set()
    average_per_hour = {}
    csv_reader = csv.reader(file)
    processed_count = 0

    for row in csv_reader:
        processed_count += 1
        if not row:
            continue
        date_str = row[0].strip()
        print(f"CSV - Date string (stripped): {date_str}")

        if len(row) < 2 or not row[1].strip() or row[1].strip().lower() in ["not_a_number", "nan"]:
            continue

        try:
            date_obj = datetime.strptime(date_str, "%d/%m/%Y %H:%M")
            date_part = date_obj.strftime("%d-%m-%Y")
            hour = date_obj.strftime("%H")
        except ValueError:
            print(f"Warning (CSV - {file_name}): Invalid date format - {date_str}")
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

        results = []
        for hour, sum_tuple in average_per_hour.items():
            avg_value = sum_tuple[0] / sum_tuple[1]
            formatted_time = f"{hour}:00:00"
            results.append([f"{date_part} {formatted_time}", f"{avg_value:.2f}"])

        file.close()
        os.remove(f"C:\\Users\\This_user\\Desktop\\לימודים\\Hadasim\\part_1\\ex_B\\{file_name}")
        print(f"Thread finished for CSV file: {file_name}. Processed {processed_count} rows, found {len(results)} average values.")
        result_queue.put(results)
        thread_finishes.set()

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
                print(f"CSV - Date string: {date_str}, Date info: {date_info}")
                if date_info:
                    date_part, time_part = date_info
                    if date_part not in files_by_date:
                        files_by_date[date_part] = open(f'C:\\Users\\This_user\\Desktop\\לימודים\\Hadasim\\part_1\\ex_B\\{date_part}.csv', mode='+a', encoding='utf-8-sig' ,newline='')
                    writer = csv.writer(files_by_date[date_part])
                    row.append(time_part)
                    writer.writerow(row)
            print(f"Processed {row_count} rows from CSV for separation.")
    elif file_extension == '.parquet':
        df = pd.read_parquet(file_path)
        print(f"Read {len(df)} rows from Parquet file for separation.")
        unique_dates = set()
        for index, row in df.iterrows():
            if len(row) < 1:
                continue
            date_value = row.iloc[0]
            date_str = str(date_value).strip()
            date_info = date_validation(date_str)
            if date_info:
                date_part, time_part = date_info
                unique_dates.add(date_part)
                if date_part not in files_by_date:
                    files_by_date[date_part] = []
                files_by_date[date_part].append(row.tolist() + [time_part])
        print(f"Found {len(unique_dates)} unique dates.")
        print(f"files_by_date: {files_by_date}")
        for date_part, rows in files_by_date.items():
            temp_df = pd.DataFrame(rows)
            files_by_date[date_part] = temp_df
            print(f"Created temporary DataFrame for date: {date_part} with {len(temp_df)} rows.")

    print("Starting thread creation...")
    result_queue = Queue()
    threads = []
    thread_count = 0

    for file_name, data in files_by_date.items():
        thread_finishes = threading.Event()
        thread_count += 1
        print(f"Creating thread {thread_count} for: {file_name}")
        if file_extension == '.csv':
            data.seek(0)
            thread = _thread.start_new_thread(process_daily_csv_file, (data, f"{file_name}.csv", result_queue, thread_finishes))
        elif file_extension == '.parquet':
            thread = _thread.start_new_thread(process_data_frame, (data, f"{file_name}.parquet_processed", result_queue, thread_finishes))
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

#file_path = 'C:\\Users\\This_user\\Desktop\\לימודים\\Hadasim\\part_1\\ex_B\\time_series.csv'
#seperate_by_date(file_path)

file_path_parquet = 'C:\\Users\\This_user\\Desktop\\לימודים\\Hadasim\\part_1\\ex_B\\time_series.parquet' # Example parquet file path
seperate_by_date(file_path_parquet)