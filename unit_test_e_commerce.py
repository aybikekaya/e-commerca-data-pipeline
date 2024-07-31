import unittest
import json
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, StringType, StructType, StructField, MapType, DoubleType, FloatType, DateType

# DataProvider class is defined 
class DataProvider:
    def __init__(self, file_paths):
        self.file_paths = file_paths

    def extract_data(self):
        combined_df = pd.DataFrame()
        for path in self.file_paths:
            source = path.split('.')[0]
            df = pd.read_json(path)
            df['source'] = source
            combined_df = pd.concat([combined_df, df], ignore_index=True)
        return combined_df

class TestECommerceDataFrames(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("TestECommerceDataFrames") \
            .getOrCreate()

        # Sample schemas
        cls.product_schema = StructType([
            StructField("product_id", IntegerType(), nullable=False),
            StructField("product_name", StringType(), nullable=False),
            StructField("price", FloatType(), nullable=False)
        ])

        cls.customer_schema = StructType([
            StructField("customer_id", IntegerType(), nullable=False),
            StructField("customer_name", StringType(), nullable=False),
            StructField("email", StringType(), nullable=False)
        ])

        cls.order_schema = StructType([
            StructField("order_id", IntegerType(), nullable=False),
            StructField("customer_id", IntegerType(), nullable=False),
            StructField("product_id", IntegerType(), nullable=False),
            StructField("quantity", IntegerType(), nullable=False),
            StructField("order_date", DateType(), nullable=False)
        ])

        # Create sample JSON files for testing
        cls.create_sample_json_files()

        # Initialize DataProvider
        cls.data_provider = DataProvider(file_paths=['product.json', 'customer.json', 'order.json'])

        # Load data
        combined_df = cls.data_provider.extract_data()
        cls.df_product = cls.spark.createDataFrame(combined_df.query('source == "product"'), schema=cls.product_schema)
        cls.df_customer = cls.spark.createDataFrame(combined_df.query('source == "customer"'), schema=cls.customer_schema)
        cls.df_order = cls.spark.createDataFrame(combined_df.query('source == "order"'), schema=cls.order_schema)

    @classmethod
    def create_sample_json_files(cls):
        # Write JSON files with sample data
        products = [
            {"product_id": 1, "product_name": "Laptop", "price": 999.99, "source": "product"},
            {"product_id": 2, "product_name": "Smartphone", "price": 499.99, "source": "product"},
            {"product_id": 3, "product_name": "Tablet", "price": 299.99, "source": "product"},
            {"product_id": 4, "product_name": "Headphones", "price": 199.99, "source": "product"}
        ]
        customers = [
            {"customer_id": 1, "customer_name": "Alice", "email": "alice@example.com", "source": "customer"},
            {"customer_id": 2, "customer_name": "Bob", "email": "bob@example.com", "source": "customer"},
            {"customer_id": 3, "customer_name": "Charlie", "email": "charlie@example.com", "source": "customer"}
        ]
        orders = [
            {"order_id": 1, "customer_id": 1, "product_id": 1, "quantity": 1, "order_date": "2023-01-01", "source": "order"},
            {"order_id": 2, "customer_id": 2, "product_id": 2, "quantity": 2, "order_date": "2023-01-02", "source": "order"},
            {"order_id": 3, "customer_id": 1, "product_id": 3, "quantity": 1, "order_date": "2023-01-03", "source": "order"},
            {"order_id": 4, "customer_id": 3, "product_id": 4, "quantity": 3, "order_date": "2023-01-04", "source": "order"}
        ]
        with open('product.json', 'w') as f:
            json.dump(products, f)
        with open('customer.json', 'w') as f:
            json.dump(customers, f)
        with open('order.json', 'w') as f:
            json.dump(orders, f)

    def test_column_existence(self):
        # Test column existence
        self.assertIn("product_id", self.df_product.columns)
        self.assertIn("product_name", self.df_product.columns)
        self.assertIn("price", self.df_product.columns)
        self.assertIn("customer_id", self.df_customer.columns)
        self.assertIn("customer_name", self.df_customer.columns)
        self.assertIn("email", self.df_customer.columns)
        self.assertIn("order_id", self.df_order.columns)
        self.assertIn("customer_id", self.df_order.columns)
        self.assertIn("product_id", self.df_order.columns)
        self.assertIn("quantity", self.df_order.columns)
        self.assertIn("order_date", self.df_order.columns)

    def test_null_values(self):
        # Check for null values
        self.assertFalse(self.df_product.filter(col("product_name").isNull() | col("price").isNull()).count() > 0)
        self.assertFalse(self.df_customer.filter(col("customer_name").isNull() | col("email").isNull()).count() > 0)
        self.assertFalse(self.df_order.filter(col("customer_id").isNull() | col("product_id").isNull() | col("quantity").isNull() | col("order_date").isNull()).count() > 0)

    def test_unique_values(self):
        # Check for uniqueness
        self.assertEqual(self.df_product.select("product_id").distinct().count(), self.df_product.count())
        self.assertEqual(self.df_customer.select("customer_id").distinct().count(), self.df_customer.count())
        self.assertEqual(self.df_order.select("order_id").distinct().count(), self.df_order.count())

    def test_data_types(self):
        # Check data types
        for name, dtype in self.df_product.dtypes:
            if name == "product_id":
                self.assertEqual(dtype, "int")
            elif name == "product_name":
                self.assertEqual(dtype, "string")
            elif name == "price":
                self.assertEqual(dtype, "float")

        for name, dtype in self.df_customer.dtypes:
            if name == "customer_id":
                self.assertEqual(dtype, "int")
            elif name == "customer_name":
                self.assertEqual(dtype, "string")
            elif name == "email":
                self.assertEqual(dtype, "string")

        for name, dtype in self.df_order.dtypes:
            if name == "order_id":
                self.assertEqual(dtype, "int")
            elif name == "customer_id":
                self.assertEqual(dtype, "int")
            elif name == "product_id":
                self.assertEqual(dtype, "int")
            elif name == "quantity":
                self.assertEqual(dtype, "int")
            elif name == "order_date":
                self.assertEqual(dtype, "date")

if __name__ == '__main__':
    unittest.main()
