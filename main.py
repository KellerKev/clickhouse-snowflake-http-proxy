import os
import snowflake.connector
from fastapi import FastAPI, Request, Response
import re
import logging

app = FastAPI()

# Empty system.settings block in Native format (0 rows)
native_empty_settings = bytes([
    0x03, 0x00,                   # columns=3, rows=0
    # column1: name(String)
    0x04, 0x6e, 0x61, 0x6d, 0x65, # "name"
    0x06, 0x53, 0x74, 0x72, 0x69, 0x6e, 0x67, # "String"
    # column2: value(String)
    0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, # "value"
    0x06, 0x53, 0x74, 0x72, 0x69, 0x6e, 0x67, # "String"
    # column3: readonly(UInt8)
    0x08, 0x72, 0x65, 0x61, 0x64, 0x6f, 0x6e, 0x6c, 0x79, # "readonly"
    0x05, 0x55, 0x49, 0x6e, 0x74, 0x38, # "UInt8"
    # end block
    0x00, 0x00
])


def get_snowflake_connection():
    return snowflake.connector.connect(
        user="",
        password="",
        account="",
        role="",
        warehouse="",
        database= "",
        schema=""

    )

def write_leb128(value: int) -> bytes:
    # Simple LEB128 encoding for small values: just one byte if value < 128.
    # For larger values, a full LEB128 implementation is needed.
    return bytes([value])

def encode_string(value: str) -> bytes:
    val_bytes = value.encode('utf-8')
    return write_leb128(len(val_bytes)) + val_bytes

def build_native_block(column_names, column_types, rows):
    """
    Build a native format block:
    - columns: int
    - rows: int
    - For each column: name(String), type(String)
    - For each row: column data
    Then a terminating block: 0x00 0x00
    """
    col_count = len(column_names)
    row_count = len(rows)
    block = write_leb128(col_count) + write_leb128(row_count)

    # Column descriptions
    for name, ctype in zip(column_names, column_types):
        block += encode_string(name)
        block += encode_string(ctype)

    # Row data (assuming all strings for simplicity)
    # Each cell: LEB128(length) + bytes
    for row in rows:
        for cell in row:
            cell_str = str(cell)
            block += encode_string(cell_str)

    # End block
    block += bytes([0x00, 0x00])
    return block

@app.api_route("/", methods=["GET", "POST"])
async def clickhouse_query(request: Request):
    print(request)
    query_param = request.query_params.get("query")
    if query_param:
        ch_query = query_param.strip()
    else:
        body = await request.body()
        ch_query = body.decode('utf-8').strip()
    
    

    # Remove FORMAT clause
    ch_query = re.sub(r"(?i)\bformat\b.*", "", ch_query).strip()
    lower_query = ch_query.lower()

    # For SELECT version(), timezone() return one row, two columns
    if "select version(), timezone()" in lower_query:
        # Columns: version(String), timezone(String)
        # One row: ("22.3.3.44 (mock)", "UTC")
        block = build_native_block(
            column_names=["version()", "timezone()"],
            column_types=["String", "String"],
            rows=[["22.3.3.44 (mock)", "UTC"]]
        )
        return Response(content=block, media_type="application/octet-stream")

    if "select version()" in lower_query:
        block = build_native_block(
            column_names=["version()"],
            column_types=["String"],
            rows=[["22.3.3.44 (mock)"]]
        )
        return Response(content=block, media_type="application/octet-stream")

    if "select timezone()" in lower_query:
        block = build_native_block(
            column_names=["timezone()"],
            column_types=["String"],
            rows=[["UTC"]]
        )
        return Response(content=block, media_type="application/octet-stream")

    # system.settings query
    if "from system.settings" in lower_query:
        return Response(content=native_empty_settings, media_type="application/octet-stream")

    # Normal query against Snowflake
    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        cur.execute(ch_query)
        rows = cur.fetchall()
        # Get column names from cursor description
        col_names = [d[0] for d in cur.description]
        # Assume all columns are type String for simplicity
        col_types = ["String"] * len(col_names)

        block = build_native_block(col_names, col_types, rows)
        
        return Response(content=block, media_type="application/octet-stream")

    except Exception as e:
        # Return an empty block or some error handling as needed
        # For simplicity, return a block with a single column 'error' and one row of e
        block = build_native_block(["error"], ["String"], [[str(e)]])
        return Response(content=block, media_type="application/octet-stream", status_code=400)
    finally:
        cur.close()
        conn.close()

