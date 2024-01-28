import pymongo
import pymysql
import re
from bson.objectid import ObjectId
from datetime import datetime

# Database credentials
mongo_db_url = "mongodb://localhost:27017/"
mongo_db_name = "stock-market"
mysql_db_host = 'localhost'
mysql_db_user = 'root'
mysql_db_password = 'mysql'
mysql_db_name = 'sm'

def camel_to_snake(name):
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()

def type_to_mysql(py_type, max_length=None):
    if py_type == 'str':
        if max_length is None:
            return 'VARCHAR(255)'
        else:
            varchar_size = min(max(50, (max_length // 50 + 1) * 50), 1000)
            return f'VARCHAR({varchar_size})' if varchar_size <= 1000 else 'TEXT'
    elif py_type == 'int':
        return 'INT'
    elif py_type == 'float':
        return 'FLOAT'
    elif py_type == 'bool':
        return 'BOOLEAN'
    elif py_type == 'ObjectId':
        return 'VARCHAR(24)'
    elif py_type == 'datetime':
        return 'DATETIME'
    elif py_type == 'Int64':
        return 'BIGINT'
    else:
        return 'VARCHAR(255)'

def create_mysql_table(mysql_cursor, collection_name, document):
    # Convert collection_name to snake_case
    collection_name = camel_to_snake(collection_name)
    # Determine the maximum length of each field in the document
    max_lengths = {camel_to_snake(key): len(str(value)) for key, value in document.items() if key not in ['_id', '_class']}

    # Adjust the structure based on the maximum lengths
    structure = {camel_to_snake(key): type_to_mysql(type(value).__name__, max_lengths.get(camel_to_snake(key))) for key, value in document.items() if key not in ['_id', '_class']}

    # Create the MySQL table based on the adjusted structure
    columns = ', '.join([f'{key} {structure[key]}' for key in structure.keys()])
    sql = f"CREATE TABLE {collection_name} (id INT AUTO_INCREMENT PRIMARY KEY, {columns})"
    print(sql,'\n')  # Print the SQL statement
    mysql_cursor.execute(sql)

def insert_into_mysql(mysql_cursor, collection_name, document):
    # Convert collection_name to snake_case
    collection_name = camel_to_snake(collection_name)
    # Remove _id and _class from the document and convert keys to snake_case
    document = {camel_to_snake(key): value for key, value in document.items() if key not in ['_id', '_class']}
    keys = ', '.join(document.keys())
    values = ', '.join(['%s' for _ in document.values()])
    sql = f"INSERT INTO {collection_name} ({keys}) VALUES ({values})"
    # Convert values to a tuple to use with execute
    values_tuple = tuple(str(value) for value in document.values())
    # Print the SQL statement with actual values
    print(sql % values_tuple)
    mysql_cursor.execute(sql, values_tuple)

# Connect to MongoDB
mongo_client = pymongo.MongoClient(mongo_db_url)
mongo_db = mongo_client[mongo_db_name]

# Use a context manager to handle the MySQL database connection
with pymysql.connect(host=mysql_db_host, user=mysql_db_user, password=mysql_db_password) as mysql_conn:
    mysql_cursor = mysql_conn.cursor()

    # Drop the database if it exists
    mysql_cursor.execute(f"DROP DATABASE IF EXISTS {mysql_db_name}")
    # Create the database
    mysql_cursor.execute(f"CREATE DATABASE {mysql_db_name}")
    # Use the database
    mysql_cursor.execute(f"USE {mysql_db_name}")

    # Iterate over all collections in MongoDB
    for collection_name in mongo_db.list_collection_names():
        print('\n\ncollection_name=', collection_name)
        collection = mongo_db[collection_name]
        # Get the structure of the collection
        document = collection.find_one()
        # Create a table in MySQL based on the collection's structure
        create_mysql_table(mysql_cursor, collection_name, document)
        # Insert data from MongoDB to MySQL
        for document in collection.find():
            insert_into_mysql(mysql_cursor, collection_name, document)

    # Commit the transaction
    mysql_conn.commit()

# Close the MongoDB connection
mongo_client.close()