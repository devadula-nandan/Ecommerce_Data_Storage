import mysql.connector
import random
import time
import datetime

# Global methods to push interact with the Database

# This method establishes the connection with the MySQL
def create_server_connection(host_name, user_name, user_password):
	# Implement the logic to create the server connection
	return mysql.connector.connect(
		host = host_name, 
		username = user_name, 
		password = user_password, 
		autocommit = True
	)

# This method will create the database
def create_switch_database(connection, db_name, switch_db):
    # For database creatio nuse this method
    # If you have created your databse using UI, no need to implement anything

	# 1.A creating a MySQL schema with the help of python

	# 1.B with appropriate fields, primary keys and foreign keys

    try : 
	    cursor = connection.cursor()
	    cursor.execute(f'CREATE DATABASE IF NOT EXISTS {db_name}')
	    cursor.execute(f'USE {db_name};')

	    tableQueries = [
	    	'''CREATE TABLE IF NOT EXISTS customers(
			customer_id VARCHAR(10) PRIMARY KEY,
			customer_name VARCHAR(45),
			customer_email VARCHAR(45),
			password VARCHAR(45),
			address VARCHAR(100)
			);
			''',
			'''
		    CREATE TABLE IF NOT EXISTS vendors(
		    	vendor_id VARCHAR(10) PRIMARY KEY,
		    	vendor_name VARCHAR(45),
		    	vendor_email VARCHAR(45),
		    	vendor_password VARCHAR(45)
		    );
		    ''',
		    '''
		    CREATE TABLE IF NOT EXISTS orders(
		    	order_id INT PRIMARY KEY,
		    	customer_id VARCHAR(10),
		    	total_value FLOAT,
		    	vendor_id VARCHAR(10),
		    	order_quantity INT,
		    	reward_point INT,
		    	FOREIGN KEY(customer_id) REFERENCES customers(customer_id),
		    	FOREIGN KEY(vendor_id) REFERENCES vendors(vendor_id)
		    );
		    ''',
		    '''
		    CREATE TABLE IF NOT EXISTS items(
		    	product_id VARCHAR(10) PRIMARY KEY,
		    	product_name VARCHAR(45),
		    	product_description VARCHAR(100),
		    	fk_vendor_id VARCHAR(10),
		    	product_price FLOAT,
		    	emi_available VARCHAR(10),
		    	FOREIGN KEY(fk_vendor_id) REFERENCES vendors(vendor_id)
			);'''
	    ]
	    for query in tableQueries : 
	    	cursor.execute(query)
    except : 
    	pass

# This method will establish the connection with the newly created DB 
def create_db_connection(host_name, user_name, user_password, db_name):
	return mysql.connector.connect(
		host = host_name, 
		username = user_name, 
		password = user_password, 
		database = db_name,
		autocommit = True
	)

# Perform all single insert statments in the specific table through a single function call

# 2.A insert method to insert a record note: this method is called in insert_many_data

def create_insert_query(connection, query) : 
	try : 
		cursor = connection.cursor()
		cursor.execute(query)
	except : 
		pass
    
# retrieving the data from the table based on the given query
def select_query(connection, query):
	try : 
		cursor = connection.cursor()
		cursor.execute(query)
		print(cursor.column_names)
		records = []
		for row in cursor : 
			print(row)
			records.append(row)
		return records
	except : 
		pass

# performing the execute many query over the table, 
# this method will help us to inert multiple records using a single instance
def insert_many_data(connection, sql, val) : 
	for query in val : 
		if sql == 'orders' : 
			query = f'INSERT INTO {sql}(order_id,customer_id,vendor_id,total_value,order_quantity,reward_point) VALUES{query};'
		elif sql == 'items' : 
			query = f'INSERT INTO {sql}(product_id,product_name,product_price,product_description,fk_vendor_id,emi_available) VALUES{query};'
		elif sql == 'customer_leaderboard' : 
			query = f'INSERT INTO {sql}(customer_id, order_id, total_value, customer_name, customer_email) VALUES{query};'
		else : 
			query = f'INSERT INTO {sql} VALUES{query};'
		create_insert_query(connection, query)