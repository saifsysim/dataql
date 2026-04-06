"""Seed a demo SQLite database with e-commerce data for immediate testing."""

import sqlite3
import random
from datetime import datetime, timedelta

DB_PATH = "demo.db"

CATEGORIES = ["Electronics", "Clothing", "Home & Kitchen", "Books", "Sports"]

PRODUCTS = [
    ("Wireless Headphones", "Electronics", 79.99),
    ("Bluetooth Speaker", "Electronics", 49.99),
    ("USB-C Hub", "Electronics", 34.99),
    ("Mechanical Keyboard", "Electronics", 129.99),
    ("Webcam HD", "Electronics", 59.99),
    ("Running Shoes", "Clothing", 89.99),
    ("Winter Jacket", "Clothing", 149.99),
    ("Cotton T-Shirt", "Clothing", 19.99),
    ("Yoga Pants", "Clothing", 39.99),
    ("Baseball Cap", "Clothing", 24.99),
    ("Coffee Maker", "Home & Kitchen", 69.99),
    ("Blender", "Home & Kitchen", 44.99),
    ("Cast Iron Skillet", "Home & Kitchen", 29.99),
    ("Air Purifier", "Home & Kitchen", 119.99),
    ("Cutting Board Set", "Home & Kitchen", 22.99),
    ("Python Handbook", "Books", 39.99),
    ("Data Science Guide", "Books", 44.99),
    ("Cooking 101", "Books", 24.99),
    ("Mystery Novel", "Books", 14.99),
    ("History of AI", "Books", 29.99),
    ("Dumbbells Set", "Sports", 54.99),
    ("Yoga Mat", "Sports", 29.99),
    ("Jump Rope", "Sports", 12.99),
    ("Basketball", "Sports", 27.99),
    ("Resistance Bands", "Sports", 19.99),
]

FIRST_NAMES = [
    "Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry",
    "Ivy", "Jack", "Karen", "Leo", "Mia", "Nathan", "Olivia", "Paul",
    "Quinn", "Rachel", "Sam", "Tina", "Uma", "Victor", "Wendy", "Xander",
    "Yara", "Zach", "Amelia", "Blake", "Chloe", "Derek",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Davis", "Miller",
    "Wilson", "Moore", "Taylor", "Anderson", "Thomas", "Jackson", "White",
    "Harris", "Martin", "Garcia", "Lee", "Clark", "Lewis",
]

CITIES = [
    ("New York", "NY"), ("Los Angeles", "CA"), ("Chicago", "IL"),
    ("Houston", "TX"), ("Phoenix", "AZ"), ("Philadelphia", "PA"),
    ("San Antonio", "TX"), ("San Diego", "CA"), ("Dallas", "TX"),
    ("Austin", "TX"), ("Seattle", "WA"), ("Denver", "CO"),
    ("Boston", "MA"), ("Portland", "OR"), ("Miami", "FL"),
]

ORDER_STATUSES = ["completed", "processing", "shipped", "cancelled", "refunded"]


def seed():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Drop existing tables
    c.execute("DROP TABLE IF EXISTS order_items")
    c.execute("DROP TABLE IF EXISTS orders")
    c.execute("DROP TABLE IF EXISTS products")
    c.execute("DROP TABLE IF EXISTS customers")

    # Create tables
    c.execute("""
        CREATE TABLE customers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            city TEXT,
            state TEXT,
            signup_date TEXT NOT NULL
        )
    """)

    c.execute("""
        CREATE TABLE products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            category TEXT NOT NULL,
            price REAL NOT NULL,
            stock_quantity INTEGER NOT NULL DEFAULT 100
        )
    """)

    c.execute("""
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id INTEGER NOT NULL,
            order_date TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'processing',
            total_amount REAL NOT NULL DEFAULT 0,
            FOREIGN KEY (customer_id) REFERENCES customers(id)
        )
    """)

    c.execute("""
        CREATE TABLE order_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL DEFAULT 1,
            unit_price REAL NOT NULL,
            FOREIGN KEY (order_id) REFERENCES orders(id),
            FOREIGN KEY (product_id) REFERENCES products(id)
        )
    """)

    # Seed customers
    random.seed(42)
    base_date = datetime(2024, 1, 1)
    customers = []
    for i in range(30):
        first = FIRST_NAMES[i]
        last = random.choice(LAST_NAMES)
        email = f"{first.lower()}.{last.lower()}@example.com"
        city, state = random.choice(CITIES)
        signup = base_date + timedelta(days=random.randint(0, 400))
        customers.append((first, last, email, city, state, signup.strftime("%Y-%m-%d")))

    c.executemany(
        "INSERT INTO customers (first_name, last_name, email, city, state, signup_date) VALUES (?,?,?,?,?,?)",
        customers,
    )

    # Seed products
    for name, category, price in PRODUCTS:
        stock = random.randint(10, 200)
        c.execute(
            "INSERT INTO products (name, category, price, stock_quantity) VALUES (?,?,?,?)",
            (name, category, price, stock),
        )

    # Seed orders + order_items
    order_id = 0
    for _ in range(120):
        customer_id = random.randint(1, 30)
        order_date = base_date + timedelta(days=random.randint(0, 450))
        status = random.choices(
            ORDER_STATUSES,
            weights=[60, 15, 15, 7, 3],
            k=1,
        )[0]

        # Pick 1-4 products for this order
        num_items = random.randint(1, 4)
        chosen_products = random.sample(range(1, len(PRODUCTS) + 1), num_items)

        total = 0.0
        items = []
        for pid in chosen_products:
            qty = random.randint(1, 3)
            price = PRODUCTS[pid - 1][2]
            total += price * qty
            items.append((pid, qty, price))

        c.execute(
            "INSERT INTO orders (customer_id, order_date, status, total_amount) VALUES (?,?,?,?)",
            (customer_id, order_date.strftime("%Y-%m-%d"), status, round(total, 2)),
        )
        real_order_id = c.lastrowid

        for pid, qty, price in items:
            c.execute(
                "INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (?,?,?,?)",
                (real_order_id, pid, qty, price),
            )

    conn.commit()
    conn.close()
    print(f"✅ Demo database seeded at {DB_PATH}")
    print(f"   → 30 customers, {len(PRODUCTS)} products, 120 orders")


if __name__ == "__main__":
    seed()
