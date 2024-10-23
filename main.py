import requests
import time
from datetime import datetime
import matplotlib.pyplot as plt
import mysql.connector
import pandas as pd
import os

# OpenWeatherMap API key and cities to monitor
API_KEY = '7c5de860ec41f4a02a0a246cad36b91e'
CITIES = ['Delhi', 'Mumbai', 'Chennai', 'Bangalore', 'Kolkata', 'Hyderabad']
BASE_URL = 'http://api.openweathermap.org/data/2.5/weather?q={}&appid={}&units=metric'
UPDATE_INERVAL = 2 # Update interval in seconds

# Database connection details
db_config = {
    'host': 'localhost',
    'user': 'SANYAM',
    'password': 'SANYAM',
    'database': 'WEATHER_SANYAM'
}

# Define thresholds for alerts
HIGH_TEMP_THRESHOLD = 35  # degrees Celsius
LOW_TEMP_THRESHOLD = 10    # degrees Celsius
HIGH_HUMIDITY_THRESHOLD = 80  # percent humidity
LOW_HUMIDITY_THRESHOLD = 30   # percent humidity

PLOTS_DIR = "plots"  # Directory to save plots

def connect_db():
    """Connect to the MySQL database.

    @return: MySQL connection object if successful, None otherwise.
    """
    try:
        connection = mysql.connector.connect(**db_config)
        return connection
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None

def get_weather_data(city):
    """Fetch weather data for a given city from OpenWeatherMap API.

    @param city: Name of the city for which weather data is requested.
    @return: JSON response containing weather data if successful, None otherwise.
    """
    response = requests.get(BASE_URL.format(city, API_KEY))
    if response.status_code == 200:
        return response.json()
    print(f"Failed to fetch data for {city}: {response.status_code}, {response.text}")
    return None

def process_weather_data(data):
    """Extract relevant fields from the weather data.

    @param data: JSON response containing weather data.
    @return: Dictionary with extracted weather information or None if input is invalid.
    """
    if data:
        return {
            'temp': data['main']['temp'],
            'feels_like': data['main']['feels_like'],
            'weather_main': data['weather'][0]['main'],
            'timestamp': data['dt'],
            'humidity': data['main']['humidity']
        }
    return None

def update_database(data):
    """Update the MySQL database with the fetched weather data.

    @param data: Dictionary containing weather data for each city.
    @return: List of results fetched from the cities table after the update.
    """
    connection = connect_db()
    if not connection:
        return

    try:
        cursor = connection.cursor()
        for city, weather_data in data.items():
            # SQL query to insert weather data into timestamp_average table
            query = """
                INSERT INTO timestamp_average (CityID, TimeStamp, Temp, Feels_Like, Humidity, Climate)
                VALUES (
                    (SELECT CityID FROM Cities WHERE CityName = %s),
                    FROM_UNIXTIME(%s),
                    %s, %s, %s, %s
                )
            """
            cursor.execute(query, (city, weather_data['timestamp'], weather_data['temp'], 
                                   weather_data['feels_like'], weather_data['humidity'], 
                                   weather_data['weather_main']))

        # Commit changes to the database
        connection.commit()
        cursor.close()
        
        # Fetch updated data for reporting
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM cities")
        results = cursor.fetchall()
        cursor.close()
        return results

    except mysql.connector.Error as err:
        print(f"Database error: {err}")
    finally:
        if connection.is_connected():
            connection.close()

def extract_date_from_df(df):
    """Extract the date from the DataFrame.

    @param df: DataFrame containing timestamp data.
    @return: Date extracted from the DataFrame.
    """
    df['Date'] = pd.to_datetime(df['TimeStamp']).dt.date
    return df['Date'].iloc[0]

def save_plot(directory, filename):
    """Ensure the directory exists and save the plot.

    @param directory: Directory path to save the plot.
    @param filename: Filename for the saved plot.
    """
    os.makedirs(directory, exist_ok=True)
    plt.savefig(os.path.join(directory, filename))
    plt.close()

def plot_daily_weather(df):
    """Generate and save plots for daily weather data.

    @param df: DataFrame containing weather data for the day.
    """
    date = extract_date_from_df(df)  # Extract date from DataFrame
    cities = df['CityName'].unique()  # Get unique city names
    
    for city in cities:
        city_data = df[df['CityName'] == city]  # Filter data for the specific city
        city_data['Serial'] = range(1, len(city_data) + 1)  # Serial number for x-axis

        # Plot temperature data
        plt.figure(figsize=(10, 6))
        plt.plot(city_data['Serial'], city_data['Temp'], label='Temperature (°C)', color='blue')
        plt.plot(city_data['Serial'], city_data['Feels_Like'], label='Feels Like (°C)', color='orange')
        plt.title(f'Temperature Over Time in {city}')
        plt.xlabel('Serial')
        plt.ylabel('Temperature (°C)')
        plt.xticks(rotation=45)
        plt.legend()
        save_plot(os.path.join(PLOTS_DIR, city), f'{date}_temperature.png')

        # Plot humidity data
        plt.figure(figsize=(10, 6))
        plt.plot(city_data['Serial'], city_data['Humidity'], label='Humidity (%)', color='green')
        plt.title(f'Humidity Over Time in {city}')
        plt.xlabel('Serial')
        plt.ylabel('Humidity (%)')
        plt.xticks(rotation=45)
        plt.legend()
        save_plot(os.path.join(PLOTS_DIR, city), f'{date}_humidity.png')

def plot_average_temp_humidity(df):
    """Generate and save plots for average temperature and humidity.

    @param df: DataFrame containing weather data for the day.
    """
    date = extract_date_from_df(df)  # Extract date from DataFrame
    avg_data = df.groupby('CityName')[['Temp', 'Humidity']].mean().reset_index()  # Calculate averages

    # Plot average temperature by city
    plt.figure(figsize=(10, 6))
    plt.bar(avg_data['CityName'], avg_data['Temp'], color='skyblue')
    plt.title('Average Temperature by City')
    plt.xlabel('City')
    plt.ylabel('Temperature (°C)')
    plt.xticks(rotation=45)
    save_plot(PLOTS_DIR, f'{date}_avg_temp.png')

    # Plot average humidity by city
    plt.figure(figsize=(10, 6))
    plt.bar(avg_data['CityName'], avg_data['Humidity'], color='lightgreen')
    plt.title('Average Humidity by City')
    plt.xlabel('City')
    plt.ylabel('Humidity (%)')
    plt.xticks(rotation=45)
    save_plot(PLOTS_DIR, f'{date}_avg_humidity.png')

def daychange():
    """Update daily averages and reset the timestamp table for the new day.

    This function moves the current day's data to the Daily_Averages table,
    truncates the timestamp_average table, and resets average values for the new day.
    """
    connection = connect_db()
    if not connection:
        return

    today_date = datetime.now().strftime('%Y-%m-%d')  # Get today's date
    try:
        cursor = connection.cursor()

        # Move data to Daily_Averages
        cursor.execute("""
            INSERT INTO Daily_Averages (CityID, Date, AvgTemp, MinTemp, MaxTemp, AvgHumidity, MostFreqClimate)
            SELECT CityID, %s, AvgTemp, MinTemp, MaxTemp, AvgHumidity, MostFreqClimate
            FROM Cities
        """, (today_date,))
        
        # Truncate timestamp_average for new day's data
        cursor.execute("TRUNCATE TABLE timestamp_average")
        
        # Update cities with new averages
        cursor.execute("""
            UPDATE cities
            SET T1 = AvgTemp,
                AvgTemp = NULL,
                MinTemp = NULL,
                MaxTemp = NULL,
                AvgHumidity = NULL,
                MostFreqClimate = NULL
        """)
        
        connection.commit()  # Commit changes to the database

    except mysql.connector.Error as err:
        print(f"Database error during day change: {err}")
    finally:
        cursor.close()  # Close cursor
        connection.close()  # Close database connection

# Main loop for updating weather data
def main():
    """Main function to update weather data at regular intervals."""
    while True:
        weather_data = {}
        for city in CITIES:
            data = get_weather_data(city)  # Fetch weather data for each city
            if data:
                processed_data = process_weather_data(data)  # Process and extract relevant data
                weather_data[city] = processed_data
        
        # Update the database with the fetched weather data
        update_database(weather_data)

        # Wait for the next update (set your own interval)
        time.sleep(UPDATE_INERVAL)  # Update every 5 minutes (300 seconds)

if __name__ == "__main__":
    main()  # Start the main function