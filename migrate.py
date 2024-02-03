import pymongo
import pymysql
import re
import json
from bson.objectid import ObjectId
from datetime import datetime

# Database credentials
mongo_db_url = "mongodb://localhost:27017/"
mongo_db_name = "stock-market"
mysql_db_host = 'localhost'
mysql_db_user = 'root'
mysql_db_password = 'mysql'
mysql_db_name = 'Stock-Market' # It will be converted to stock_market

DEFAULT_VARCHAR_SIZE = 25
MAX_VARCHAR_LENGTH = 1000

def camel_to_snake(name):
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()

def isChildKeyPresentInValue(childKey, valueValues):
    for value in valueValues:
        if childKey==value:
            return True
    return False

def type_to_mysql(column_name, py_type, max_length):
    if py_type == 'str':
        varchar_size = min(max(DEFAULT_VARCHAR_SIZE, (max_length // DEFAULT_VARCHAR_SIZE + 1) * DEFAULT_VARCHAR_SIZE), MAX_VARCHAR_LENGTH)
        return f'VARCHAR({varchar_size})' if varchar_size <= MAX_VARCHAR_LENGTH else 'TEXT'
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
    elif py_type == 'Decimal128':
        return 'DECIMAL(38, 3)' 
    elif py_type == 'list':
        return 'TEXT'  # Lists are serialized as JSON strings
    elif py_type == 'bytes':
        return 'BLOB'
    elif py_type == 'NoneType':
        return 'TEXT'
    elif py_type == 're.Pattern':
        return 'TEXT'  # Regular expressions can be stored as text
    else:
        return 'VARCHAR(255)'
def process_nested_document(doc, prefix=''):
    structure = {}
    for childKey, value in doc.items():
        if isinstance(value, dict):
            valueValues = value.values()
            if isChildKeyPresentInValue(childKey, valueValues):
                valueKeys = value.keys()
                for inner_key in valueKeys:
                    new_key = f"{prefix}_{camel_to_snake(inner_key)}" if prefix else camel_to_snake(inner_key)
                    innerKeyValue = value[inner_key]
                    max_length = len(str(innerKeyValue))
                    structure[new_key] = type_to_mysql(new_key, type(innerKeyValue).__name__, max_length)
                    if isinstance(innerKeyValue, dict):
                        structure.update(process_nested_document(value, new_key))
            else:
                # add the prefix here
                new_key = f"{prefix}_{camel_to_snake(childKey)}" if prefix else camel_to_snake(childKey)
                structure.update(process_nested_document(value, new_key))
        else:
            max_length = len(str(value))
            new_key = f"{prefix}_{camel_to_snake(childKey)}" if prefix else camel_to_snake(childKey)
            structure[new_key] = type_to_mysql(new_key, type(value).__name__, max_length)
    return structure

def convert_nested_document(doc, prefix=''):
    new_document = {}
    for childKey, doc in doc.items():
        if isinstance(doc, dict):
            valueValues = doc.values()
            if isChildKeyPresentInValue(childKey, valueValues):
                valueKeys=doc.keys()
                for inner_key in valueKeys:
                    new_key = f"{prefix}_{camel_to_snake(inner_key)}" if prefix else camel_to_snake(inner_key)
                    innerKeyValue=doc[inner_key]
                    if isinstance(innerKeyValue, ObjectId):
                        new_document[new_key] = str(doc)
                    elif isinstance(innerKeyValue, datetime):
                        new_document[new_key] = doc.strftime('%Y-%m-%d %H:%M:%S')
                    elif isinstance(innerKeyValue, list):
                        new_document[new_key] = json.dumps(doc, default=str)
                    elif isinstance(innerKeyValue, dict):
                        new_document.update(convert_nested_document(innerKeyValue, new_key))
                    elif doc is None:
                        new_document[new_key] = 'NULL'
                    elif isinstance(innerKeyValue, bool):
                        new_document[new_key] = 1 if doc else 0
                    else:
                        new_document[new_key] = innerKeyValue
            else:
                new_key = f"{prefix}_{camel_to_snake(childKey)}" if prefix else camel_to_snake(childKey)
                new_document.update(convert_nested_document(doc, new_key))
        else:
            new_key = f"{prefix}_{camel_to_snake(childKey)}" if prefix else camel_to_snake(childKey)
            if isinstance(doc, ObjectId):
                new_document[new_key] = str(doc)
            elif isinstance(doc, datetime):
                new_document[new_key] = doc.strftime('%Y-%m-%d %H:%M:%S')
            elif isinstance(doc, list):
                new_document[new_key] = json.dumps(doc, default=str)
            elif isinstance(doc, dict):
                new_document.update(convert_nested_document(doc, new_key))
            elif doc is None:
                new_document[new_key] = 'NULL'
            elif isinstance(doc, bool):
                new_document[new_key] = 1 if doc else 0
            else:
                new_document[new_key] = doc
    return new_document

# Convert the document to a MySQL-friendly format
def convert_document(document, prefix=''):
    new_document = {}
    for key, value in document.items():
        new_key = f"{prefix}_{key}" if prefix else key
        # Handle ObjectId
        if isinstance(value, ObjectId):
            new_document[new_key] = str(value)
        # Handle Date
        elif isinstance(value, datetime):
            new_document[new_key] = value.strftime('%Y-%m-%d %H:%M:%S')
        # Handle Array
        elif isinstance(value, list):
             # Serialize the list as a JSON string
            new_document[new_key] = json.dumps(value, default=str)
        # Handle Nested Document
        elif isinstance(value, dict):
            new_document.update(convert_nested_document(value, new_key))
        # Handle Null
        elif value is None:
            new_document[new_key] = 'NULL'
        # Handle Boolean
        elif isinstance(value, bool):
            new_document[new_key] = 1 if value else 0
        # Handle all other types
        else:
            new_document[new_key] = value
    return new_document

def enquote(identifier):
    return f"`{identifier}`"

def create_mysql_table(mysql_cursor, collection_name, document):
    collection_name = camel_to_snake(collection_name)
    # Determine the maximum length of each field in the document
    max_lengths = {camel_to_snake(key): len(str(value)) for key, value in document.items() if key not in ['_id', '_class']}

    # Adjust the structure based on the maximum lengths
    structure = {}
    for key, value in document.items():
        if key not in ['_id', '_class']:
            if isinstance(value, dict):
                structure.update(process_nested_document(value, camel_to_snake(key)))
            else:
                structure[camel_to_snake(key)] = type_to_mysql(camel_to_snake(key), type(value).__name__, max_lengths.get(camel_to_snake(key)))

    column_definitions = []
    # Create the MySQL table based on the adjusted structure
    if "id" not in structure:
        column_definitions.append("id INT AUTO_INCREMENT PRIMARY KEY")
    column_definitions.extend([f'{enquote(key)} {structure[key]}' for key in structure.keys()])
    sql = f"CREATE TABLE {enquote(collection_name)} ({', '.join(column_definitions)})"
    print(sql,'\n')  # Print the SQL statement
    mysql_cursor.execute(sql)

def insert_into_mysql(mysql_cursor, collection_name, document):
    collection_name = camel_to_snake(collection_name)
    # Remove _id and _class from the document and convert keys to snake_case
    document = {camel_to_snake(key): value for key, value in document.items() if key not in ['_id', '_class']}
    # Convert the document to a MySQL-friendly format
    document = convert_document(document)
    keys = ', '.join(enquote(key) for key in document.keys())
    values = ', '.join(['%s' for _ in document.values()])
    sql = f"INSERT INTO {enquote(collection_name)} ({keys}) VALUES ({values})"
    # Convert values to a tuple to use with execute
    values_tuple = tuple(str(value) for value in document.values())
    quoted_values_tuple = tuple(f"'{value}'" if isinstance(value, str) else value for value in values_tuple)
    # Print the SQL statement with actual values
    print(sql % quoted_values_tuple)
    while True:
        try:
            mysql_cursor.execute(sql, values_tuple)
            break  # Success, exit the loop
        except pymysql.err.OperationalError as e:
            if 'Unknown column' in str(e):
                # Handle multiple missing fields
                missing_fields = re.findall(r"Unknown column '([^']+)'", str(e))
                for field in missing_fields:
                    field_length = len(str(document[field]))
                    mongo_type =  type(document[field]).__name__
                    field_type = type_to_mysql(field, mongo_type, field_length)
                    mysql_cursor.execute(f"ALTER TABLE {collection_name} ADD COLUMN {field} {field_type}")
            else:
                raise
        except pymysql.err.DataError as e:
            # If a Data Too Long error occurs, increase the length of the affected field
            if 'Data too long' in str(e):
                field = re.search(r"'(.+)'", str(e)).group(1)
                # Get the current data type and length of the field
                mysql_cursor.execute(f"SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{collection_name}' AND COLUMN_NAME = '{field}'")
                current_type, current_length = mysql_cursor.fetchone()
                # Decide the new type and length based on the current type and length
                if current_type == 'varchar':
                    new_length = current_length * 2
                    if new_length > MAX_VARCHAR_LENGTH:
                        new_type = 'TEXT'
                    else:
                        new_type = f'VARCHAR({new_length})'
                elif current_type == 'text':
                    new_type = 'MEDIUMTEXT'
                elif current_type == 'mediumtext':
                    new_type = 'LONGTEXT'
                else:
                    raise ValueError(f"Cannot increase size of field {field} of type {current_type}")
                # Alter the table to change the type of the field
                mysql_cursor.execute(f"ALTER TABLE {collection_name} MODIFY {field} {new_type}")
            else:
                raise

# Connect to MongoDB
mongo_client = pymongo.MongoClient(mongo_db_url)
mongo_db = mongo_client[mongo_db_name]

# Use a context manager to handle the MySQL database connection
with pymysql.connect(host=mysql_db_host, user=mysql_db_user, password=mysql_db_password) as mysql_conn:
    mysql_cursor = mysql_conn.cursor()
    mysql_db_name = mysql_db_name.replace('-', '_')
    # Drop the database if it exists
    mysql_cursor.execute(f"DROP DATABASE IF EXISTS {mysql_db_name}")
    # Create the database
    mysql_cursor.execute(f"CREATE DATABASE {mysql_db_name}")
    # Use the database
    mysql_cursor.execute(f"USE {mysql_db_name}")

    # Iterate over all collections in MongoDB
    for collection_name in mongo_db.list_collection_names():
        print('\ncollection_name=', collection_name)
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