import database as db

# below is the setup for accessing password through environment variables
# make .env file in the same folder as this file and enter :
# PASSWORD=your_password_here
# totally optional

import os
from dotenv import load_dotenv
load_dotenv()

PASSWORD = os.getenv('PASSWORD') # you can hardcode the password here and remove the 3 lines above

# Driver code
if __name__ == "__main__":

    """
    Please enter the necessary information related to the DB at this place. 
    Please change PW and ROOT based on the configuration of your own system. 
    """
    PW = PASSWORD # IMPORTANT! Put your MySQL Terminal password here.
    ROOT = "root"
    DB = "Ecommerce" # This is the name of the database we will create in the next step - call it whatever you like.
    LOCALHOST = "localhost"
    connection = db.create_server_connection(LOCALHOST, ROOT, PW)

    # creating the schema in the DB 
    db.create_switch_database(connection, DB, DB)

    # Start implementing your task as mentioned in the problem statement 
    # Implement all the test cases and test them by running this file

    # 2.B inserting 5 records into the orders table
    # we can insert single or multiple records to the desired table
    db.insert_many_data(connection, 'orders', [
        (101, '7', '4', 20000, 3, 100),
        (102, '6', '2', 42355, 5, 300),
        (103, '1', '1', 100, 1, 10),
        (104, '8', '3', 500000, 6, 1000),
        (105, '5', '5', 9349, 2, 250)    
    ])
    print('Inserted records successfully')

    # 2.C running a select query and printing the orders table

    print('\nPrinting Orders Table')
    db.select_query(connection, 'SELECT * FROM ORDERS;')

    # 3.A finding minimum and maximum value order

    minMaxQuery = '''
    SELECT 
    MIN(total_value) AS MinimumValueOrder,
    MAX(total_value) AS MaximumValueOrder
    FROM Orders;
    '''
    print('\nFinding maximum and minimum value orders')
    db.select_query(connection, minMaxQuery)

    # 3.B finding the orders which have more value than average order value

    moreThanAvgQuery = '''
    SELECT * FROM Orders
    WHERE total_value >= (
        SELECT AVG(total_value) FROM Orders
    );
    '''
    print('\nFinding the orders which have more value than average value of orders')
    db.select_query(connection, moreThanAvgQuery)

    # 3.C creating a leaderboard table of customers on the basis of their highest value order.

    leaderboardQuery = '''
    SELECT c.customer_id, o.order_id, MAX(o.total_value) AS max_total_value, c.customer_name, c.customer_email 
    FROM Customers c INNER JOIN Orders o ON c.customer_id = o.customer_id
    GROUP BY c.customer_id
    ORDER BY max_total_value DESC;
    '''
    print('\nRanking every customer by their highest total value for an order and creating a leaderboard table for it')
    records = db.select_query(connection, leaderboardQuery)
    cursor = connection.cursor()
    leaderboardCreateQuery = '''
    CREATE TABLE IF NOT EXISTS customer_leaderboard(
        leaderboard_id INT PRIMARY KEY AUTO_INCREMENT,
        customer_id VARCHAR(10),
        order_id INT,
        total_value FLOAT,
        customer_name VARCHAR(45),
        customer_email VARCHAR(45),
        FOREIGN KEY(customer_id) REFERENCES customers(customer_id),
        FOREIGN KEY(order_id) REFERENCES orders(order_id) 
    );
    '''
    cursor.execute(leaderboardCreateQuery)
    db.insert_many_data(connection, 'customer_leaderboard', records)