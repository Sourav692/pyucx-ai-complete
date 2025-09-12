from databricks import sql

# Connection parameters
connection = sql.connect(
    server_hostname="<your-workspace-host>",
    http_path="<your-cluster-or-sql-warehouse-http-path>",
    access_token="<your-access-token>"
)

cursor = connection.cursor()

# Create a Unity Catalog table
cursor.execute("""
CREATE TABLE IF NOT EXISTS main.default.customers (
    id INT,
    name STRING,
    email STRING
)
USING delta
""")

print("✅ Table created successfully")

cursor.close()
connection.close()
