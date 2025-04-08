import _thread
import queue
import threading  

def find_top_n(n, error_queue):
    result = {}
    while not error_queue.empty():
        dict = error_queue.get()
        for error_type, error_count in dict.items():
            if error_type in result:
                result[error_type] += error_count
            else:
                result[error_type] = error_count
    sorted_errors = sorted(result.items(), key=lambda x: x[1], reverse=True)
    top_n = []
    try:
        for i in range(n):
            top_n.append(sorted_errors[i][0])
    except Exception as e:
        return("there is no top n in file. n is too much big. "+ str(e))

    return top_n


def count_error_types(errors_list, type_errors_queue, thread_finishes):
    count_types = {}
    for error in errors_list:
        if error in count_types:
            count_types[error] += 1
        else:
            count_types[error] = 1
    type_errors_queue.put(count_types)
    thread_finishes.set() 

def seperate_file(file_path, n):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            block_size = 100000
            end_of_file = False
            type_errors_queue = queue.Queue()
            threads = []  # ניצור רשימה שתכיל את כל התהליכים

            while not end_of_file:
                errors_list = []
                for i in range(block_size):
                    line = file.readline()
                    if not line:
                        end_of_file = True
                        break
                    errors_list.append(line.strip()[40:-1])

                # יצירת Event לכל תהליך
                thread_finishes = threading.Event()
                thread = _thread.start_new_thread(count_error_types, (errors_list, type_errors_queue, thread_finishes))
                threads.append(thread_finishes) 

            # חכה עד שכל התהליכים יסתיימו
            for thread in threads:
                thread.wait()  # נחכה שהתהליך יסיים

            # קריאה ל-find_top_n לאחר שכל התהליכים יסתיימו
            return find_top_n(n, type_errors_queue)

    except Exception as e:
        print("An error occurred while opening the file:", str(e))


file_path = 'C:\\Users\\This_user\\Desktop\\לימודים\\Hadasim\\part_1\\ex_A\\logs.txt'
top_n=3
print(seperate_file(file_path, top_n))
