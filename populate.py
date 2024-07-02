from pymongo import MongoClient
from faker import Faker
import random

# Connect to the local port forwarded MongoDB
client = MongoClient('mongodb://user:password@localhost:27017/?authMechanism=SCRAM-SHA-256&authSource=auth-database-name&directConnection=true')
db = client['testdb']

# Initialize Faker
fake = Faker()

def generate_users(count):
    users = []
    for _ in range(count):
        users.append({
            'user_id': fake.uuid4(),
            'name': fake.name(),
            'email': fake.email(),
            'created_at': fake.date_time_this_decade()
        })
    return users

def generate_products(count):
    products = []
    for _ in range(count):
        products.append({
            'product_id': fake.uuid4(),
            'name': fake.word(),
            'description': fake.text(),
            'price': round(random.uniform(1.0, 1000.0), 2),
            'created_at': fake.date_time_this_decade()
        })
    return products

def generate_orders(count, user_ids, product_ids):
    orders = []
    for _ in range(count):
        orders.append({
            'order_id': fake.uuid4(),
            'user_id': random.choice(user_ids),
            'product_ids': random.sample(product_ids, random.randint(1, 5)),
            'total_amount': round(random.uniform(10.0, 5000.0), 2),
            'created_at': fake.date_time_this_decade()
        })
    return orders

def insert_data():
    user_count = 100000
    product_count = 500000
    order_count = 200000

    users = generate_users(user_count)
    db.users.insert_many(users)
    print(f'Inserted {user_count} users')

    products = generate_products(product_count)
    db.products.insert_many(products)
    print(f'Inserted {product_count} products')

    user_ids = [user['user_id'] for user in users]
    product_ids = [product['product_id'] for product in products]

    orders = generate_orders(order_count, user_ids, product_ids)
    db.orders.insert_many(orders)
    print(f'Inserted {order_count} orders')

def test_connection():
    # Test inserting a document
    test_doc = {"name": "Test User", "email": "test@example.com"}
    db.test_collection.insert_one(test_doc)
    print("Test document inserted")

    # Test retrieving the document
    retrieved_doc = db.test_collection.find_one({"name": "Test User"})
    print(f"Retrieved document: {retrieved_doc}")

# Uncomment this line to run the full data insertion
insert_data()

# Run the test connection function
# test_connection()
print("Data generation complete")
