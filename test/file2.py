from databricks import sql

connection = sql.connect(
    server_hostname="<your-workspace-host>",
    http_path="<your-cluster-or-sql-warehouse-http-path>",
    access_token="<your-access-token>"
)

cursor = connection.cursor()

# Insert data
cursor.execute("""
INSERT INTO main.default.customers (id, name, email)
VALUES (1, 'Alice', 'alice@example.com'),
       (2, 'Bob', 'bob@example.com')
""")
print("✅ Records inserted")

# Update data
cursor.execute("""
UPDATE main.default.customers
SET email = 'alice@newdomain.com'
WHERE id = 1
""")
print("✅ Record updated")

cursor.close()
connection.close()