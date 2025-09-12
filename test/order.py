from databricks import sql

connection = sql.connect(
    server_hostname="<your-workspace-host>",
    http_path="<your-cluster-or-sql-warehouse-http-path>",
    access_token="<your-access-token>"
)

cursor = connection.cursor()

# Create table if not exists
cursor.execute("""
CREATE TABLE IF NOT EXISTS main.default.orders (
    order_id INT,
    customer_id INT,
    order_date DATE
)
USING delta
""")

# Insert values
cursor.execute("""
INSERT INTO main.default.orders (order_id, customer_id, order_date)
VALUES (5001, 1, current_date()),
       (5002, 2, current_date())
""")
print("✅ Inserted into orders")

# Update value
cursor.execute("""
UPDATE main.default.orders
SET customer_id = 3
WHERE order_id = 5001
""")
print("✅ Updated order")

# Select values
cursor.execute("SELECT * FROM main.default.orders")
rows = cursor.fetchall()
print("📊 Orders Table:")
for row in rows:
    print(row)

cursor.close()
connection.close()
