from clickhouse_connect import get_client

client = get_client(host='localhost', port=8000, interface='http')



# Test a normal query that Snowflake can handle:
res_hello = client.query("select current_version()")
print("Hello result:", res_hello.result_set[0][0])
