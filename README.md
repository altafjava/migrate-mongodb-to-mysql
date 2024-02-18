# MongoDB to MySQL Migration Script

This Python script migrates data from a MongoDB database to a MySQL database. It iterates over all collections in the MongoDB database, creates corresponding tables in the MySQL database, and inserts the data.

## Prerequisites

Ensure that you have Python installed on your machine. You also need to have MongoDB and MySQL servers running.

## Installation

1. Clone the repository to your local machine.
2. Navigate to the project directory.
3. Install the required Python packages:

    ```bash
    pip install pymongo
    pip install pymysql
    ```

## Configuration

Update the MongoDB and MySQL database credentials in the `config.py` file:

```python
# Source Database Configuration
mongo_config = {
    'url': 'mongodb://localhost:27017/',
    'db_name': 'sample_guides',
}

# Destination Database Configuration
mysql_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'mysql',
}

mysql_db_name = 'Test_DB-MySQL' # This will be converted to snake case(test_db_mysql)
```

## Usage
Run the script with the following command:
```bash
python migrate.py
```
The script will connect to the MongoDB database, iterate over all collections, It then connects to the MySQL server, **drops the existing database if it exists**, and creates a new one. For each MongoDB collection, the script creates a corresponding table and column with **snake_case** names based on the MongoDB collections' structure, and insert data accordingly. It skips the `_id` and `_class` columns during table creation and data insertion based on the configuration settings `SKIP_ID_FIELD` and `SKIP_CLASS_FIELD` respectively.

## Note
This script assumes that the MongoDB and MySQL servers are running on localhost. If your servers are running on different hosts, update the host information in the database credentials.