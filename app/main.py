import sys



DATABASE_OFFSET_ENCODING = 56


def get_database_encoding(database_file):
    database_file.seek(DATABASE_OFFSET_ENCODING)
    encoding = int.from_bytes(database_file.read(4), byteorder="big")
    if encoding == 1:
        return "UTF-8"
    elif encoding == 2:
        return "UTF-16le"
    elif encoding == 3:
        return "UTF-16be"


def read_int(file, size):
    return int.from_bytes(file.read(size), byteorder="big")


def read_varint(file):
    val = 0
    USE_NEXT_BYTE = 0x80
    BITS_TO_USE = 0x7F
    for _ in range(9):
        byte = read_int(file, 1)
        val = (val << 7) | (byte & BITS_TO_USE)
        if byte & USE_NEXT_BYTE == 0:
            break
    return val


def parse_record_body(srl_type, file):
    if srl_type == 0:
        return None
    elif srl_type == 1:
        return read_int(file, 1)
    elif srl_type == 2:
        return read_int(file, 2)
    elif srl_type == 3:
        return read_int(file, 3)
    elif srl_type == 4:
        return read_int(file, 4)
    elif srl_type == 5:
        return read_int(file, 6)
    elif srl_type == 6:
        return read_int(file, 8)
    elif srl_type >= 12 and srl_type % 2 == 0:
        datalen = (srl_type - 12) // 2
        return file.read(datalen).decode()
    elif srl_type >= 13 and srl_type % 2 == 1:
        datalen = (srl_type - 13) // 2
        return file.read(datalen).decode()
    else:
        return None


def parse_cell(c_ptr, file):
    file.seek(c_ptr)
    format_hdr_start = file.tell()
    format_hdr_sz = read_varint(file)
    serial_types = []
    format_body_start = format_hdr_start + format_hdr_sz
    while file.tell() < format_body_start:
        serial_types.append(read_varint(file))
    record = []
    for srl_type in serial_types:

        result = parse_record_body(srl_type, file)
        if result:
            record.append(result)
    return record


database_file_path = sys.argv[1]
command = sys.argv[2]
HEADER_OFFSET = 100

if command == ".dbinfo":
    with open(database_file_path, "rb") as database_file:
        table_content = 0
        database_file.seek(HEADER_OFFSET + 3)
        # reserved_region = int.from_bytes(database_file.read(1), byteorder="big")
        number_of_cells = int.from_bytes(database_file.read(2), byteorder="big")
elif command == ".tables":
    with open(database_file_path, "rb") as database_file:
        # You can use print statements as follows for debugging, they'll be visible when running tests.
        database_file.seek(HEADER_OFFSET)
        page_type = int.from_bytes(database_file.read(1), byteorder="big")
        database_file.seek(HEADER_OFFSET + 3)
        number_of_cells = int.from_bytes(database_file.read(2), byteorder="big")
        database_file.seek(HEADER_OFFSET + 8)
        cell_pointers = [
            int.from_bytes(database_file.read(2), "big") for _ in range(number_of_cells)
        ]
        records = []
        for each_cell in cell_pointers:
            record = parse_cell(each_cell, database_file)
            records.append(record)
        tbl_name = []
        for each_record in records:
            if each_record[2] != "sqlite_sequence":
                tbl_name.append(each_record[2])
        print(*tbl_name)
else:
    print(f"Invalid command: {command}")
# TODO: the ability to understand and code this concept by reading the doc properly.

# if __name__ == "__main__":
#     pass
