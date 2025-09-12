from databricks import sql

connection = sql.connect(
    server_hostname="<your-workspace-host>",
    http_path="<your-cluster-or-sql-warehouse-http-path>",
    access_token="<your-access-token>"
)

cursor = connection.cursor()

# Create table if not exists
cursor.execute("""
CREATE TABLE IF NOT EXISTS main.default.products (
    product_id INT,
    product_name STRING,
    price DOUBLE
)
USING delta
""")

# Insert values
cursor.execute("""
INSERT INTO main.default.products (product_id, product_name, price)
VALUES (101, 'Laptop', 1200.00),
       (102, 'Headphones', 150.00)
""")
print("✅ Inserted into products")

# Update value
cursor.execute("""
UPDATE main.default.products
SET price = 1100.00
WHERE product_id = 101
""")
print("✅ Updated product price")

# Select values
cursor.execute("SELECT * FROM main.default.products")
rows = cursor.fetchall()
print("📊 Products Table:")
for row in rows:
    print(row)

cursor.close()
connection.close()
