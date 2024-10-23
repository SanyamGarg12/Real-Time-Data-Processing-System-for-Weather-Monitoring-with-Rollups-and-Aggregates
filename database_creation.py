import mysql.connector
from mysql.connector import Error
from main import db_config

# Function to create database and tables


def create_database_and_tables():
    try:
        # Connect to MySQL server
        connection = mysql.connector.connect(
            host=db_config['host'],
            user=db_config['user'],
            password=db_config['password']
        )

        if connection.is_connected():
            cursor = connection.cursor()

            # Create the database
            cursor.execute("CREATE DATABASE IF NOT EXISTS WEATHER_SANYAM;")
            # Use the newly created database
            cursor.execute("USE WEATHER_SANYAM;")

            # Create the Cities table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS Cities (
                    CityID INT AUTO_INCREMENT PRIMARY KEY,
                    CityName VARCHAR(255) NOT NULL,
                    Longitude DECIMAL(9,6),
                    Latitude DECIMAL(9,6),
                    AvgTempLast7Days DECIMAL(5,2),
                    T1 DECIMAL(5,2),
                    AvgTemp DECIMAL(5,2),
                    MinTemp DECIMAL(5,2),
                    MaxTemp DECIMAL(5,2),
                    AvgHumidity DECIMAL(5,2),
                    MostFreqClimate VARCHAR(255)
                );
            ''')

            # Create the Daily_Averages table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS Daily_Averages (
                    CityID INT,
                    Date DATE,
                    AvgTemp DECIMAL(5,2),
                    MaxTemp DECIMAL(5,2),
                    MinTemp DECIMAL(5,2),
                    AvgHumidity DECIMAL(5,2), 
                    MostFreqClimate VARCHAR(255),
                    PRIMARY KEY (CityID, Date),
                    FOREIGN KEY (CityID) REFERENCES Cities(CityID) ON DELETE CASCADE
                );
            ''')

            # Create the Timestamp_Averages table
            cursor.execute('''
                CREATE TABLE timestamp_average (
                    CityID INT,
                    TimeStamp DATETIME NOT NULL,
                    Temp DECIMAL(5,2),
                    Feels_Like DECIMAL(5,2),
                    Humidity DECIMAL(5,2),
                    Climate VARCHAR(255),
                    FOREIGN KEY (CityID) REFERENCES Cities(CityID) -- Assuming Cities table exists
                );
            ''')

            print("Database and tables created successfully.")

    except Error as e:
        print(f"Error: {e}")

    finally:
        # Close the connection
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection closed.")


# Execute the function
create_database_and_tables()
