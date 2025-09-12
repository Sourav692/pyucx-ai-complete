from databricks import sql

connection = sql.connect(
    server_hostname="<your-workspace-host>",
    http_path="<your-cluster-or-sql-warehouse-http-path>",
    access_token="<your-access-token>"
)

cursor = connection.cursor()

# Create table if not exists
cursor.execute("""
CREATE TABLE IF NOT EXISTS main.default.employees (
    emp_id INT,
    emp_name STRING,
    department STRING
)
USING delta
""")

# Insert values
cursor.execute("""
INSERT INTO main.default.employees (emp_id, emp_name, department)
VALUES (201, 'John Doe', 'Finance'),
       (202, 'Jane Smith', 'IT')
""")
print("✅ Inserted into employees")

# Update value
cursor.execute("""
UPDATE main.default.employees
SET department = 'HR'
WHERE emp_id = 201
""")
print("✅ Updated employee department")

# Select values
cursor.execute("SELECT * FROM main.default.employees")
rows = cursor.fetchall()
print("📊 Employees Table:")
for row in rows:
    print(row)

cursor.close()
connection.close()
