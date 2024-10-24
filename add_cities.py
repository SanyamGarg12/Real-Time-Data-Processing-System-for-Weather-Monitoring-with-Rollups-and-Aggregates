import mysql.connector
from mysql.connector import Error
from config import db_config

# Function to insert cities into the Cities table


def insert_cities():
    # Sample city data
    cities_data = [
        {
            'CityName': 'Delhi',
            'Longitude': 77.2167,
            'Latitude': 28.6667,
            'AvgTempLast7Days': None,
            'T1': None  # You can fill this based on your requirements
        },
        {
            'CityName': 'Mumbai',
            'Longitude': 72.8479,
            'Latitude': 19.0144,
            'AvgTempLast7Days': None,
            'T1': None
        },
        {
            'CityName': 'Chennai',
            'Longitude': 80.2785,
            'Latitude': 13.0878,
            'AvgTempLast7Days': None,
            'T1': None
        },
        {
            'CityName': 'Bangalore',
            'Longitude': 77.6033,
            'Latitude': 12.9762,
            'AvgTempLast7Days': None,
            'T1': None
        },
        {
            'CityName': 'Kolkata',
            'Longitude': 88.3697,
            'Latitude': 22.5697,
            'AvgTempLast7Days': None,
            'T1': None
        },
        {
            'CityName': 'Hyderabad',
            'Longitude': 78.4744,
            'Latitude': 17.3753,
            'AvgTempLast7Days': None,
            'T1': None
        }
    ]

    try:
        # Connect to MySQL server
        connection = mysql.connector.connect(**db_config)

        if connection.is_connected():
            cursor = connection.cursor()

            # SQL insert statement
            insert_query = '''
                INSERT INTO Cities (CityName, Longitude, Latitude, AvgTempLast7Days, T1)
                SELECT * FROM (SELECT %s AS CityName, %s AS Longitude, %s AS Latitude, %s AS AvgTempLast7Days, %s AS T1) AS tmp
                WHERE NOT EXISTS (
                    SELECT CityName FROM Cities WHERE CityName = %s
                );
            '''

            # Insert each city into the Cities table
            for city in cities_data:
                cursor.execute(insert_query, (
                    city['CityName'],
                    city['Longitude'],
                    city['Latitude'],
                    city['AvgTempLast7Days'],
                    city['T1'],
                    city['CityName']
                ))

            # Commit the transaction
            connection.commit()
            print("Cities inserted successfully.")

    except Error as e:
        print(f"Error: {e}")

    finally:
        # Close the connection
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection closed.")


if __name__ == '__main__':
    # Execute the function
    insert_cities()

    """
-- Disable foreign key checks to avoid constraint errors during the truncation
SET FOREIGN_KEY_CHECKS = 0;

-- Truncate tables to remove all records and reset auto-increment values
TRUNCATE TABLE timestamp_average;
TRUNCATE TABLE Daily_Averages;
TRUNCATE TABLE Cities;

-- Re-enable foreign key checks
SET FOREIGN_KEY_CHECKS = 1;

    """

    """
-- Select all records from cities table
SELECT * FROM cities;

-- Select all records from Daily_Averages table
SELECT * FROM Daily_Averages;

-- Select all records from timestamp_average table
SELECT * FROM timestamp_average;

    """

    # ALTER TABLE daily_averages DROP PRIMARY KEY;
    # ALTER TABLE daily_averages ADD PRIMARY KEY (CityID, Date);
