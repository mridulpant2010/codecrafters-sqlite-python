import sys

from dataclasses import dataclass
import sqlparse

# import sqlparse - available if you need it!

database_file_path = sys.argv[1]
command = sys.argv[2]

# def get_db_metadata(database_file_path,options):
#     offset_mapping = {
#         'page_size': 16,
#         'number_of_tables': 
#     }
#      with open(database_file_path, "rb") as database_file:
#         # You can use print statements as follows for debugging, they'll be visible when running tests.
#         print("Logs from your program will appear here!")
#         if options=='page_size':
#             database_file.seek(offset_mapping)
#             page_size = int.from_bytes(database_file.read(2), byteorder="big")
        

if command == ".dbinfo":
    with open(database_file_path, "rb") as database_file:
        # You can use print statements as follows for debugging, they'll be visible when running tests.
        print("Logs from your program will appear here!")
        table_count=0
        # Uncomment this to pass the first stage
        database_file.seek(16)  # Skip the first 16 bytes of the header
        page_size = int.from_bytes(database_file.read(2), byteorder="big")
        print(f"database page size: {page_size}")
        for each_content in database_file:
            table_count+=each_content.count(b"CREATE TABLE")
            print(f"number of tables: {table_count}")
else:
    print(f"Invalid command: {command}")


# if __name__ == "__main__":
#     pass