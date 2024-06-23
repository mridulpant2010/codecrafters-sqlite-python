import sys

database_file_path = sys.argv[1]
command = sys.argv[2]
HEADER_OFFSET = 100


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
        print("INVALID SERIAL TYPE")
        return None


def parse_cell(c_ptr, file):
    file.seek(c_ptr)
    payload_size = read_varint(file)
    row_id = read_varint(file)
    format_hdr_start = file.tell()
    format_hdr_sz = read_varint(file)
    serial_types = []
    format_body_start = format_hdr_start + format_hdr_sz
    # print(payload_size, row_id, format_hdr_sz)
    while file.tell() < format_body_start:
        serial_types.append(read_varint(file))
    # print(serial_types)
    record = []
    for srl_type in serial_types:
        record.append(parse_record_body(srl_type, file))
    # print(record)
    return record


def read_file_contents():
    with open(database_file_path, "rb") as database_file:
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
        sqlite_schema_rows = []
        for each_record in records:
            if each_record[2] != "sqlite_sequence":
                tbl_name.append(each_record[2])
                sqlite_schema_rows.append(
                    {
                        "type": each_record[0],
                        "name": each_record[2],
                        "tbl_name": each_record[1],
                        "rootpage": each_record[3],
                        "sql": each_record[4],
                    }
                )
        return tbl_name, sqlite_schema_rows


if command == ".dbinfo":
    with open(database_file_path, "rb") as database_file:
        database_file.seek(16)
        page_size = int.from_bytes(database_file.read(2), byteorder="big")
        database_file.seek(103)
        table_amt = int.from_bytes(database_file.read(2), byteorder="big")
        print(f"database page size: {page_size}\nnumber of tables: {table_amt}")
elif command == ".tables":
    tbl_name, _ = read_file_contents()
    print(*tbl_name)

elif "count(*)" in command:
    _, sqlite_schema_rows = read_file_contents()
    table_name = command.split(" ")[-1]
    for each_schema in sqlite_schema_rows:
        if each_schema["tbl_name"] == table_name:
            # print(each_schema["tbl_name"],each_schema["rootpage"])
            rootpage = each_schema["rootpage"]

    with open(database_file_path, "rb") as database_file:
        database_file.seek(16)
        page_size = int.from_bytes(database_file.read(2), byteorder="big")
        offset = (rootpage - 1) * page_size
        database_file.seek(offset)
        value_internal = int.from_bytes(database_file.read(1), byteorder="big")
        if value_internal == 13:
            # leaf page
            database_file.seek(offset + 3)
            cell_content = int.from_bytes(database_file.read(2), byteorder="big")
            print(cell_content)
    # print(sqlite_schema_rows)
else:
    print(f"Invalid command: {command}")
