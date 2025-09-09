import mysql.connector
import csv

# Setup database connection

def get_db_connection():
    connection = None
    try:
        connection = mysql.connector.connect(
            user='your_username',
            password='your_password',
            host='your_host',
            port='3306',
            database='your_database'
        )
    except Exception as error:
        print("Error while connecting to database", error)
    return connection


# Create Sales table

def create_sales_table(connection):
    cursor = connection.cursor()
    ddl = """
    CREATE TABLE IF NOT EXISTS sales (
        ticket_id INT,
        trans_date DATE,
        event_id INT,
        event_name VARCHAR(50),
        event_date DATE,
        event_type VARCHAR(10),
        event_city VARCHAR(20),
        customer_id INT,
        price DECIMAL(10,2),
        num_tickets INT
    );
    """
    cursor.execute(ddl)
    connection.commit()
    cursor.close()

# Load CSV to table

def load_third_party(connection, file_path_csv):
    cursor = connection.cursor()

    #cursor.execute("DELETE FROM sales") ## maybe take this out

    with open(file_path_csv, mode='r') as file:
        csv_reader = csv.reader(file)
        next(csv_reader)

        for row in csv_reader:
            insert_query = """
                INSERT INTO sales (ticket_id, trans_date, event_id, event_name, event_date, event_type, event_city, customer_id, price, num_tickets)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            cursor.execute(insert_query, row)

        connection.commit()
        cursor.close()
        print(f"Successfully loaded data from {file_path_csv}")


# Display statistical information 

def query_popular_tickets(connection):
    sql_statement = """
        SELECT event_name, SUM(num_tickets) AS total_sales
        FROM sales
        WHERE event_date >= event_date -INTERVAL 1 MONTH
        GROUP BY event_name
        ORDER BY total_sales DESC
        LIMIT 3
    """

    cursor = connection.cursor()
    cursor.execute(sql_statement)
    records = cursor.fetchall()
    cursor.close()

    return records

# Dislpay the results
def display_popular_tickets(records):
    print("Here are the most popular tickets in the past month:")
    for record in records:
        print(f"- {record[0]}")



if __name__ == "__main__":
    csv_file = 'path_to_your_csv_file.csv'
    conn = get_db_connection()
    if conn:
        create_sales_table(conn)
        load_third_party(conn, csv_file)
        popular_events = query_popular_tickets(conn)
        display_popular_tickets(popular_events)
        conn.close()