import pymongo
import pymysql
import re

# Database credentials
mongo_db_url = "mongodb://localhost:27017/"
mongo_db_name = "stock-market"
mysql_db_host = 'localhost'
mysql_db_user = 'root'
mysql_db_password = 'mysql'
mysql_db_name = 'sm'

DEFAULT_VARCHAR_SIZE = 25
MAX_VARCHAR_LENGTH = 1000

def camel_to_snake(name):
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()

def type_to_mysql(py_type, max_length=None):
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
    else:
        return 'VARCHAR(255)'

def enquote(identifier):
    return f"`{identifier}`"

def create_mysql_table(mysql_cursor, collection_name, document):
    # Convert collection_name to snake_case
    collection_name = camel_to_snake(collection_name)
    # Determine the maximum length of each field in the document
    max_lengths = {camel_to_snake(key): len(str(value)) for key, value in document.items() if key not in ['_id', '_class']}

    # Adjust the structure based on the maximum lengths
    structure = {camel_to_snake(key): type_to_mysql(type(value).__name__, max_lengths.get(camel_to_snake(key))) for key, value in document.items() if key not in ['_id', '_class']}

    # Create the MySQL table based on the adjusted structure
    columns = ', '.join([f'{enquote(key)} {structure[key]}' for key in structure.keys()])
    sql = f"CREATE TABLE {enquote(collection_name)} (id INT AUTO_INCREMENT PRIMARY KEY, {columns})"
    print(sql,'\n')  # Print the SQL statement
    mysql_cursor.execute(sql)

def insert_into_mysql(mysql_cursor, collection_name, document):
    # Convert collection_name to snake_case
    collection_name = camel_to_snake(collection_name)
    # Remove _id and _class from the document and convert keys to snake_case
    document = {camel_to_snake(key): value for key, value in document.items() if key not in ['_id', '_class']}
    # keys = ', '.join(document.keys())
    keys = ', '.join(enquote(key) for key in document.keys())
    values = ', '.join(['%s' for _ in document.values()])
    # sql = f"INSERT INTO {collection_name} ({keys}) VALUES ({values})"
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
                    field_type = type_to_mysql(type(document[field]).__name__, field_length)
                    mysql_cursor.execute(f"ALTER TABLE {collection_name} ADD COLUMN {field} {field_type}")
            else:
                raise
        except pymysql.err.DataError as e:
            # If a Data Too Long error occurs, increase the length of the affected field
            if 'Data too long' in str(e):
                field = re.search(r"'(.+)'", str(e)).group(1)
                # Get the current length of the field
                mysql_cursor.execute(f"SELECT CHARACTER_MAXIMUM_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{collection_name}' AND COLUMN_NAME = '{field}'")
                current_length = mysql_cursor.fetchone()[0]
                # Double the current length
                new_length = current_length * 2
                if new_length > MAX_VARCHAR_LENGTH:
                    # Modify the field to TEXT if it reaches MAX_VARCHAR_LENGTH
                    mysql_cursor.execute(f"ALTER TABLE {collection_name} MODIFY {field} TEXT")
                else:
                    # Alter the table to increase the length of the field
                    mysql_cursor.execute(f"ALTER TABLE {collection_name} MODIFY {field} VARCHAR({new_length})")
            else:
                raise

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