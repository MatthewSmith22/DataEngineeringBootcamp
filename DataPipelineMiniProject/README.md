# Ticket Sales Data Pipeline

## Prerequisites
- Python 3.x
- MySQL database
- MySQL user credentials with permissions to create and insert tables
- `mysql-connector-python` library

## Setup Instructions

1. Install MySQL Connector Python Package:
   
   `pip install mysql-connector-python`

2. Update Database Credentials
```
   connection = mysql.connector.connect(
       user='your_username'
       password='your_password'
       host='your_host'
       port='3306'
       database='your_database'
   )
```

3. Update File Path
```
    if __name__ == "__main__":
         csv_file = 'path_to_your_csv_file.csv'
```

4. Run the Script

   `python pipeline.py`

The script will:
- Connect to the MySQL database
- Create the `sales` table
- Load the ticket sales data from the CSV
- Query and display the 3 most popular events

       
