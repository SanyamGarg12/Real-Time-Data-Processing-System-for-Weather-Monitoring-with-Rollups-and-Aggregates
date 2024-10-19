import mysql.connector
from mysql.connector import Error
from datetime import datetime, timedelta

# Database connection details
MYSQL_HOST = 'localhost'  # Change this to your MySQL server hostname
MYSQL_USER = 'weather_user'       # Your MySQL username
MYSQL_PASSWORD = 'new_password'  # Your MySQL password
MYSQL_DATABASE = 'weather_data'  # The database you created

def connect_to_db():
    """Connect to the MySQL database and return the connection object."""
    try:
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE
        )
        if conn.is_connected():
            print(f"Connected to MySQL database {MYSQL_DATABASE}")
            return conn
    except Error as e:
        print(f"Error: {e}")
        return None

def fetch_weather_data(conn, days=5):
    """Fetch weather data for the last 'days' days for each city."""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    # Query to fetch weather data
    query = '''
    SELECT city, date, avg_temp, max_temp, min_temp, dominant_condition
    FROM weather_summary
    WHERE date BETWEEN %s AND %s
    ORDER BY city, date
    '''
    
    cursor = conn.cursor()
    cursor.execute(query, (start_date.date(), end_date.date()))
    results = cursor.fetchall()
    
    cursor.close()
    return results

def describe_weather_data(data):
    """Format and print the weather data."""
    if not data:
        print("No weather data found.")
        return

    current_city = None
    for row in data:
        city, date, avg_temp, max_temp, min_temp, dominant_condition = row
        
        # Check if we have switched cities for formatting
        if city != current_city:
            if current_city is not None:
                print()  # Print a new line for separation
            current_city = city
            print(f"Weather Data for {city} over the last 5 days:\n{'-' * 40}")
        
        print(f"Date: {date}, Avg Temp: {avg_temp:.2f}°C, Max Temp: {max_temp:.2f}°C, Min Temp: {min_temp:.2f}°C, Dominant Condition: {dominant_condition}")

def main():
    conn = connect_to_db()
    if conn:
        weather_data = fetch_weather_data(conn, days=5)
        describe_weather_data(weather_data)
        
        # Close the database connection
        conn.close()
        print("MySQL connection closed.")

if __name__ == "__main__":
    main()
