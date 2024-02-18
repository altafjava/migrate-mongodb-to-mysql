# Source Database Configuration
mongo_config = {
    'url': 'mongodb://localhost:27017/',
    # 'db_name': 'sample_airbnb',
    # 'db_name': 'sample_analytics',
    # 'db_name': 'sample_geospatial',
    'db_name': 'sample_guides',
    # 'db_name': 'sample_mflix',
    # 'db_name': 'sample_restaurants',
    # 'db_name': 'sample_supplies',
    # 'db_name': 'sample_training'
    # 'db_name': 'sample_weatherdata',
}

# Destination Database Configuration
mysql_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'mysql',
}

mysql_db_name = 'Test_DB-MySQL' # This will be converted to snake case(test_db_mysql)