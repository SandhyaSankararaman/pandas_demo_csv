import pandas as pd
import sqlite3 as db

# Read data from spreadsheets
spreadsheet_0 = pd.read_csv(r"C:\Task Resources-20240207T090009Z-001\Task Resources\shipping_data_0.csv")
spreadsheet_1 = pd.read_csv(r"C:\Task Resources-20240207T090009Z-001\Task Resources\shipping_data_1.csv")
spreadsheet_2 = pd.read_csv(r"C:\\Task Resources-20240207T090009Z-001\Task Resources\shipping_data_2.csv")

# Connect to the database
conn = db.connect(r"C:\Task Resources-20240207T090009Z-001\Task Resources\shipment_database2.db")
cursor = conn.cursor()

# Process Spreadsheet 0
spreadsheet_0.to_sql('product', conn, index=False, if_exists='replace')

# Create 'shipment' table if it doesn't exist
cursor.execute('''
    CREATE TABLE IF NOT EXISTS shipment (
        shipment_identifier TEXT,
        product_id INTEGER,
        quantity INTEGER,
        on_time TEXT,
        origin TEXT,
        destination TEXT,
        driver_identifier TEXT
    )
''')

# Process Spreadsheet 1
for index, row in spreadsheet_1.iterrows():
    shipment_identifier = row['shipment_identifier']
    product_name = row['product']
    on_time = row['on_time']

    # Check if the product already exists in the product table
    cursor.execute("SELECT product FROM product WHERE product = ?", (product_name,))
    existing_product = cursor.fetchone()

    if not existing_product:
        # If the product doesn't exist, insert it into the product table
        cursor.execute("INSERT INTO product (product) VALUES (?)", (product_name,))
    
    # Get the id of the product
    cursor.execute("SELECT rowid FROM product WHERE product = ?", (product_name,))
    product_id = cursor.fetchone()[0]

    # Assuming 'quantity' is available in spreadsheet_1, adjust this line accordingly
    quantity = row.get('quantity', 0)  # Default to 0 if 'quantity' is not present

    cursor.execute("INSERT INTO shipment (shipment_identifier, product_id, quantity, on_time) VALUES (?, ?, ?, ?)",
                   (shipment_identifier, product_id, quantity, on_time))
# Process Spreadsheet 2
print("Spreadsheet_2 Columns:", spreadsheet_2.columns)
for index, row in spreadsheet_2.iterrows():
    shipment_identifier = row['shipment_identifier']
    origin = row['origin_warehouse']
    destination = row['destination_store']
    driver_identifier = row['driver_identifier']

    # Assuming 'quantity' is available in spreadsheet_2, adjust this line accordingly
    quantity = row.get('product_quantity', 0)  # Default to 0 if 'product_quantity' is not present

    # Check if the shipment already exists in the shipment table
    cursor.execute("SELECT * FROM shipment WHERE shipment_identifier = ?", (shipment_identifier,))
    existing_shipment = cursor.fetchone()

    if existing_shipment:
        # Update existing shipment
        cursor.execute("UPDATE shipment SET origin = ?, destination = ?, driver_identifier = ?, quantity = ? WHERE shipment_identifier = ?",
                       (origin, destination, driver_identifier, quantity, shipment_identifier))
    else:
        # Insert new shipment
        cursor.execute("INSERT INTO shipment (shipment_identifier, origin, destination, driver_identifier, quantity) VALUES (?, ?, ?, ?, ?)",
                       (shipment_identifier, origin, destination, driver_identifier, quantity))

# Commit changes
conn.commit()

# Print some rows from the shipment table for verification
cursor.execute("SELECT * FROM shipment LIMIT 5")
print(cursor.fetchall())



# Close the connection
conn.close()
