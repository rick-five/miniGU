import minigu_python
print('Python bindings imported successfully')

# Test creating a PyMiniGU instance
db = minigu_python.PyMiniGU()
print('PyMiniGU instance created successfully')

# Test initializing the database
try:
    db.init()
    print('Database initialized successfully')
except Exception as e:
    print(f'Error initializing database: {e}')

# Test executing a simple query
try:
    # Using a simple query that might work
    result = db.execute("SHOW PROCEDURES")
    print('Query executed successfully')
    print(f'Result: {result}')
except Exception as e:
    print(f'Error executing query (expected during development): {e}')

# Test closing the database
try:
    db.close()
    print('Database closed successfully')
except Exception as e:
    print(f'Error closing database: {e}')