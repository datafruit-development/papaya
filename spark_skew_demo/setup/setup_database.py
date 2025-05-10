import psycopg2
from psycopg2 import sql
import random
from faker import Faker
import time
from datetime import datetime, timedelta

# PostgreSQL connection parameters
DB_PARAMS = {
    "host": "localhost",
    "port": 5433,
    "database": "skew_db",
    "user": "demo_user",
    "password": "demo_password"
}

NUM_TRANSACTIONS = 1000000  # Target: ~1 million rows
SKEW_FACTOR = 0.80  # 80% of transactions will have region_id 1
SKEWED_REGION_ID = 1
NUM_REGIONS = 10 # Should match regions.csv

fake = Faker()

def connect_db():
    """Connects to the PostgreSQL database."""
    conn = None
    for i in range(5): # Retry connection
        try:
            conn = psycopg2.connect(**DB_PARAMS)
            print("Successfully connected to PostgreSQL.")
            return conn
        except psycopg2.OperationalError as e:
            print(f"Connection attempt {i+1} failed: {e}")
            if i < 4:
                time.sleep(5) # Wait before retrying
            else:
                raise
    return conn


def create_tables(conn):
    """Creates the transactions and regions tables."""
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS transactions;")
        cur.execute("DROP TABLE IF EXISTS regions;") # If you want to manage regions also in DB

        # For this demo, regions will be from CSV, but you could create it here too.
        # For simplicity, we only create the transactions table.
        # If you want to join with a DB table for regions:
        # cur.execute("""
        # CREATE TABLE regions (
        # region_id INTEGER PRIMARY KEY,
        # region_name VARCHAR(100),
        # country VARCHAR(50)
        # );
        # """)

        cur.execute("""
        CREATE TABLE transactions (
            transaction_id SERIAL PRIMARY KEY,
            customer_id INTEGER,
            product_id INTEGER,
            amount DECIMAL(10, 2),
            transaction_date TIMESTAMP,
            region_id INTEGER
        );
        """)
        conn.commit()
        print("Tables (transactions) created successfully.")

def generate_transactions_batch(batch_size):
    """Generates a batch of transaction data."""
    transactions = []
    for _ in range(batch_size):
        customer_id = fake.random_int(min=1, max=100000)
        product_id = fake.random_int(min=1, max=1000)
        amount = round(random.uniform(5.0, 500.0), 2)
        transaction_date = fake.date_time_between(start_date="-2y", end_date="now")

        # Introduce skew
        if random.random() < SKEW_FACTOR:
            region_id = SKEWED_REGION_ID
        else:
            region_id = random.randint(1, NUM_REGIONS) # Assuming 10 regions

        transactions.append((customer_id, product_id, amount, transaction_date, region_id))
    return transactions

def populate_data(conn):
    """Populates the transactions table with skewed data."""
    batch_size = 10000
    total_inserted = 0
    print(f"Populating transactions table with ~{NUM_TRANSACTIONS} rows...")

    with conn.cursor() as cur:
        insert_query = sql.SQL("""
            INSERT INTO transactions (customer_id, product_id, amount, transaction_date, region_id)
            VALUES (%s, %s, %s, %s, %s);
        """)
        while total_inserted < NUM_TRANSACTIONS:
            batch_data = generate_transactions_batch(batch_size)
            cur.executemany(insert_query, batch_data)
            conn.commit()
            total_inserted += len(batch_data)
            print(f"Inserted {total_inserted}/{NUM_TRANSACTIONS} transactions...")
    print("Transaction data populated successfully.")

def main():
    conn = None
    try:
        conn = connect_db()
        if conn:
            create_tables(conn)
            populate_data(conn)
            print("Database setup complete.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()