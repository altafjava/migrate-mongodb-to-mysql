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

Update the MongoDB and MySQL database credentials in the script:

```python
mongo_db_url = "mongodb://localhost:27017/"
mongo_db_name = "stock-market"
mysql_db_host = 'localhost'
mysql_db_user = 'root'
mysql_db_password = 'mysql'
mysql_db_name = 'sm'
```

## Usage
Run the script with the following command:
```pyhton
python migrate.py
```
The script will connect to the MongoDB database, iterate over all collections, create corresponding tables in the MySQL database, and insert the data.

## Note
This script assumes that the MongoDB and MySQL servers are running on localhost. If your servers are running on different hosts, update the host information in the database credentials.