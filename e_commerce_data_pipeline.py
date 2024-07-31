import pandas as pd
import json
from google.colab import files
import mysql.connector
import psycopg2

# Upload multiple files
uploaded = files.upload()
file_paths = list(uploaded.keys())  # Get the list of uploaded file names

class DataProvider:
    def __init__(self, file_paths):
        self.file_paths = file_paths

    def extract_data(self):
        all_data = []
        for file_path in self.file_paths:
            with open(file_path, 'r') as file:
                data = json.load(file)
            df = pd.DataFrame(data).T
            all_data.append(df)
        combined_df = pd.concat(all_data, ignore_index=True)
        return combined_df

class Category:
    def __init__(self, category_id, category_name):
        self.category_id = category_id
        self.category_name = category_name

    def __repr__(self):
        return f"Category(id={self.category_id}, name={self.category_name})"

class Product:
    def __init__(self, product_id, product_name, category_id, price, stock_quantity):
        self.product_id = product_id
        self.product_name = product_name
        self.category_id = category_id
        self.price = price
        self.stock_quantity = stock_quantity

    def __repr__(self):
        return (f"Product(id={self.product_id}, name={self.product_name}, "
                f"category_id={self.category_id}, price={self.price}, "
                f"stock_quantity={self.stock_quantity})")

class Order:
    def __init__(self, order_id, product_id, quantity, order_date, customer_id):
        self.order_id = order_id
        self.product_id = product_id
        self.quantity = quantity
        self.order_date = order_date
        self.customer_id = customer_id

    def __repr__(self):
        return (f"Order(id={self.order_id}, product_id={self.product_id}, "
                f"quantity={self.quantity}, order_date={self.order_date}, "
                f"customer_id={self.customer_id})")

class Transformer:
    def __init__(self, dataframe):
        self.df = dataframe

    def transform(self):
        # Transform categories
        self.categories_df = pd.DataFrame(self.df['category'].apply(pd.Series))
        self.categories_df = self.categories_df.rename(columns={'id': 'category_id', 'name': 'category_name'})

        # Transform products
        self.products = []
        for _, row in self.df.iterrows():
            category_id = row['category']['id']

            product = Product(
                product_id=row['product_id'],
                product_name=row['name'],
                category_id=category_id,
                price=row['price'],
                stock_quantity=row['stock_quantity']
            )
            self.products.append(product)

        # Transform orders
        self.orders_df = pd.DataFrame(self.df['orders'].apply(pd.Series))
        self.orders_df = self.orders_df.rename(columns={
            'id': 'order_id', 'product_id': 'product_id', 'quantity': 'quantity', 
            'order_date': 'order_date', 'customer_id': 'customer_id'})

    def get_transformed_data(self):
        return {
            'categories': self.categories_df.drop_duplicates().to_dict(orient='records'),
            'products': [vars(product) for product in self.products],
            'orders': self.orders_df.drop_duplicates().to_dict(orient='records')
        }

def connect_to_mysql():
    return mysql.connector.connect(
        host="your_mysql_host",
        user="your_mysql_user",
        password="your_mysql_password",
        database="your_mysql_database"
    )

def connect_to_postgresql():
    return psycopg2.connect(
        host="your_postgresql_host",
        user="your_postgresql_user",
        password="your_postgresql_password",
        database="your_postgresql_database"
    )

def create_tables_mysql(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Category (
        category_id INT PRIMARY KEY,
        category_name VARCHAR(255)
    );
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Product (
        product_id INT PRIMARY KEY,
        product_name VARCHAR(255),
        category_id INT,
        price DECIMAL(10, 2),
        stock_quantity INT,
        FOREIGN KEY (category_id) REFERENCES Category(category_id)
    );
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Orders (
        order_id INT PRIMARY KEY,
        product_id INT,
        quantity INT,
        order_date DATE,
        customer_id INT,
        FOREIGN KEY (product_id) REFERENCES Product(product_id)
    );
    """)

def create_tables_postgresql(cursor):
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Category (
        category_id SERIAL PRIMARY KEY,
        category_name VARCHAR(255)
    );
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Product (
        product_id SERIAL PRIMARY KEY,
        product_name VARCHAR(255),
        category_id INT,
        price DECIMAL(10, 2),
        stock_quantity INT,
        FOREIGN KEY (category_id) REFERENCES Category(category_id)
    );
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Orders (
        order_id SERIAL PRIMARY KEY,
        product_id INT,
        quantity INT,
        order_date DATE,
        customer_id INT,
        FOREIGN KEY (product_id) REFERENCES Product(product_id)
    );
    """)

def insert_data_mysql(cursor, data):
    for category in data['categories']:
        cursor.execute("INSERT IGNORE INTO Category (category_id, category_name) VALUES (%s, %s)",
                       (category['category_id'], category['category_name']))
    for product in data['products']:
        cursor.execute("""
        INSERT IGNORE INTO Product (product_id, product_name, category_id, price, stock_quantity)
        VALUES (%s, %s, %s, %s, %s)""",
                       (product['product_id'], product['product_name'], product['category_id'], product['price'], product['stock_quantity']))
    for order in data['orders']:
        cursor.execute("""
        INSERT IGNORE INTO Orders (order_id, product_id, quantity, order_date, customer_id)
        VALUES (%s, %s, %s, %s, %s)""",
                       (order['order_id'], order['product_id'], order['quantity'], order['order_date'], order['customer_id']))

def insert_data_postgresql(cursor, data):
    for category in data['categories']:
        cursor.execute("INSERT INTO Category (category_id, category_name) VALUES (%s, %s) ON CONFLICT (category_id) DO NOTHING",
                       (category['category_id'], category['category_name']))
    for product in data['products']:
        cursor.execute("""
        INSERT INTO Product (product_id, product_name, category_id, price, stock_quantity)
        VALUES (%s, %s, %s, %s, %s) ON CONFLICT (product_id) DO NOTHING""",
                       (product['product_id'], product['product_name'], product['category_id'], product['price'], product['stock_quantity']))
    for order in data['orders']:
        cursor.execute("""
        INSERT INTO Orders (order_id, product_id, quantity, order_date, customer_id)
        VALUES (%s, %s, %s, %s, %s) ON CONFLICT (order_id) DO NOTHING""",
                       (order['order_id'], order['product_id'], order['quantity'], order['order_date'], order['customer_id']))

def main():
    provider = DataProvider(file_paths)
    df = provider.extract_data()
    transformer = Transformer(df)
    transformer.transform()
    transformed_data = transformer.get_transformed_data()

    # Connect to MySQL and PostgreSQL databases
    mysql_conn = connect_to_mysql()
    mysql_cursor = mysql_conn.cursor()
    postgresql_conn = connect_to_postgresql()
    postgresql_cursor = postgresql_conn.cursor()

    # Create tables
    create_tables_mysql(mysql_cursor)
    create_tables_postgresql(postgresql_cursor)

    # Insert data
    insert_data_mysql(mysql_cursor, transformed_data)
    insert_data_postgresql(postgresql_cursor, transformed_data)

    # Commit and close connections
    mysql_conn.commit()
    mysql_cursor.close()
    mysql_conn.close()
    postgresql_conn.commit()
    postgresql_cursor.close()
    postgresql_conn.close()

    print("Data inserted successfully into MySQL and PostgreSQL databases.")

if __name__ == "__main__":
    main()
