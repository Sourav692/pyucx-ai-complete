from databricks import sql

connection = sql.connect(
    server_hostname="<your-workspace-host>",
    http_path="<your-cluster-or-sql-warehouse-http-path>",
    access_token="<your-access-token>"
)

cursor = connection.cursor()

# Delete a record
cursor.execute("""
DELETE FROM main.default.customers
WHERE id = 2
""")
print("✅ Record deleted")

# Drop the table
cursor.execute("DROP TABLE IF EXISTS main.default.customers")
print("✅ Table dropped")

cursor.close()
connection.close()
