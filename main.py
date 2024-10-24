import requests
import time
from datetime import datetime, timezone
from collections import defaultdict
from datetime import date
import matplotlib.pyplot as plt
import mysql.connector
from mysql.connector import Error
from add_cities import insert_cities
import pandas as pd
import seaborn as sns
import os
from config import db_config

API_KEY = '7c5de860ec41f4a02a0a246cad36b91e'
CITIES = ['Delhi', 'Mumbai', 'Chennai', 'Bangalore', 'Kolkata', 'Hyderabad']
BASE_URL = 'http://api.openweathermap.org/data/2.5/weather?q={}&appid={}&units=metric'
UPDATE_INTERVAL = 1  # in seconds

# Database connection details
MYSQL_HOST = 'localhost'  # Change this to your MySQL server hostname
MYSQL_USER = 'SANYAM'       # Your MySQL username
MYSQL_PASSWORD = 'SANYAM'  # Your MySQL password
MYSQL_DATABASE = 'WEATHER_SANYAM'  # The database you created

# Define thresholds for alerts
HIGH_TEMP_THRESHOLD = 35  # degrees Celsius
LOW_TEMP_THRESHOLD = 10    # degrees Celsius
HIGH_HUMIDITY_THRESHOLD = 80  # percent humidity
LOW_HUMIDITY_THRESHOLD = 30   # percent humidity

PLOTS_DIR = "plots"

table_city = 'Cities'
table_daily_avg = 'Daily_Averages'
table_timestamp_avg = 'timestamp_average'


# convert  temp from Kelvin to Celsius or Fahrenheit
def convert_temperature(temp_kelvin, unit="Celsius"):
    if unit == "Celsius":
        return temp_kelvin - 273.15
    elif unit == "Fahrenheit":
        return ((convert_temperature(temp_kelvin)) * 9/5) + 32
    else:
        return temp_kelvin

# Use API call to get weather data for a city
def get_weather_data(city):
    response = requests.get(BASE_URL.format(city, API_KEY))
    if response.status_code == 200:
        return response.json()
    else:
        print(
            f"Failed to fetch data for {city}: {response.status_code}, {response.text}")
        return None

# converts the data into a dictionary with required fields
def process_weather_data(data):
    if data:
        return {
            'temp' : data['main']['temp'],
            'feels_like' : data['main']['feels_like'],
            'weather_main' : data['weather'][0]['main'],
            'timestamp' : data['dt'],
            'humidity' : data['main']['humidity']
        }
    return None

# updates data for an interval in the database and returns the results after considering the data
def update_database(data):
    try:
        connection = mysql.connector.connect(**db_config)
        if connection.is_connected():

            cursor = connection.cursor()

            for city, data in data.items():
                timestamp = data['timestamp']
                temp = data['temp']
                feels_like = data['feels_like']
                climate = data['weather_main']
                humidity = data['humidity']

                insert_query = """
                INSERT INTO timestamp_average (CityID, TimeStamp, Temp, Feels_Like, Humidity, Climate)
                VALUES (
                    (SELECT CityID FROM Cities WHERE CityName = %s), 
                    FROM_UNIXTIME(%s), 
                    %s, 
                    %s, 
                    %s,
                    %s
                )
                """

                cursor.execute(insert_query, (city, timestamp, temp, feels_like, humidity, climate))

            update_query = """
            UPDATE cities c
            JOIN (
                SELECT 
                    CityID,
                    AVG(Temp) AS AvgTemp,
                    MIN(Temp) AS MinTemp,
                    MAX(Temp) AS MaxTemp,
                    AVG(Humidity) AS AvgHumidity,
                    (
                        SELECT Climate 
                        FROM timestamp_average ta2 
                        WHERE ta2.CityID = ta.CityID 
                        GROUP BY Climate 
                        ORDER BY COUNT(*) DESC 
                        LIMIT 1
                    ) AS MostFreqClimate
                FROM 
                    timestamp_average ta
                GROUP BY 
                    CityID
            ) ta ON c.CityID = ta.CityID
            SET 
                c.t1 = c.AvgTemp,
                c.AvgTemp = ta.AvgTemp,
                c.MinTemp = ta.MinTemp,
                c.MaxTemp = ta.MaxTemp,
                c.AvgHumidity = ta.AvgHumidity,
                c.MostFreqClimate = ta.MostFreqClimate;
            """
            cursor.execute(update_query)

            # Commit the changes
            connection.commit()

            # Fetching data from the database
            cursor.execute("SELECT * FROM cities")
            results = cursor.fetchall()

            cursor.close()
            connection.close()
            return results

    except Error as e:
        print(f"Error: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def extract_date_from_df(df):
    # Ensure TimeStamp column is in datetime format
    df['Date'] = pd.to_datetime(df['TimeStamp']).dt.date
    return df['Date'].iloc[0]

def plot_daily_weather(df):
    # Extract the date from the DataFrame
    date = extract_date_from_df(df)
    df['Serial'] = df.groupby('CityName').cumcount() + 1
    cities = df['CityName'].unique()
    for city in cities:
        city_data = df[df['CityName'] == city]
        # add serial number column to the data
        # city_data.loc[:, 'Serial'] = range(1, len(city_data) + 1)
        


        plt.figure(figsize=(10, 6))
        plt.plot(city_data['Serial'], city_data['Temp'], label='Temperature (°C)', color='blue')
        plt.plot(city_data['Serial'], city_data['Feels_Like'], label='Feels Like (°C)', color='orange')
        plt.title(f'Temperature Over Time in {city}')
        plt.xlabel('Serial')
        plt.ylabel('Temperature (°C)')
        plt.xticks(rotation=45)
        plt.legend()
        plt.tight_layout()
        plt.savefig(os.path.join(PLOTS_DIR, city, f'{date}_temperature.png'))
        plt.close()
        
        # Plot humidity over time
        plt.figure(figsize=(10, 6))
        plt.plot(city_data['Serial'], city_data['Humidity'], label='Humidity (%)', color='green')
        plt.title(f'Humidity Over Time in {city}')
        plt.xlabel('Serial')
        plt.ylabel('Humidity (%)')
        plt.xticks(rotation=45)
        plt.legend()
        plt.tight_layout()
        plt.savefig(os.path.join(PLOTS_DIR, city, f'{date}_humidity.png'))
        plt.close()

def plot_average_temp_humidity(df):
    # Extract the date from the DataFrame
    date = extract_date_from_df(df)

    # Group by CityName and calculate average Temp and Humidity
    avg_data = df.groupby('CityName')[['Temp', 'Humidity']].mean().reset_index()
    print(avg_data)
    # Create a new directory if it doesn't exist
    # os.makedirs(PLOTS_DIR, exist_ok=True)
    
    # Plot average temperature
    plt.figure(figsize=(10, 6))
    plt.bar(avg_data['CityName'], avg_data['Temp'], color='skyblue')
    plt.title('Average Temperature by City')
    plt.xlabel('City')
    plt.ylabel('Temperature (°C)')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(os.path.join(PLOTS_DIR, f'{date}_avg_temp.png'))
    plt.close()

    # Plot average humidity
    plt.figure(figsize=(10, 6))
    plt.bar(avg_data['CityName'], avg_data['Humidity'], color='lightgreen')
    plt.title('Average Humidity by City')
    plt.xlabel('City')
    plt.ylabel('Humidity (%)')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(os.path.join(PLOTS_DIR, f'{date}_avg_humidity.png'))
    plt.close()

def plot_temperature_distribution(df):
    # Extract the date from the DataFrame
    date = extract_date_from_df(df)

    # Create a new directory if it doesn't exist
    # os.makedirs(PLOTS_DIR, exist_ok=True)
    
    # Plot temperature distribution for all cities
    plt.figure(figsize=(10, 6))
    plt.hist(df['Temp'], bins=20, color='skyblue', edgecolor='black')
    plt.title('Temperature Distribution for All Cities')
    plt.xlabel('Temperature (°C)')
    plt.ylabel('Frequency')
    plt.tight_layout()
    plt.savefig(os.path.join(PLOTS_DIR, f'{date}_temperature_distribution.png'))
    plt.close()

def plot_climate_distribution(df):
    # Extract the date from the DataFrame
    date = extract_date_from_df(df)

    # Create a new directory if it doesn't exist
    # os.makedirs(PLOTS_DIR, exist_ok=True)
    
    # Plot climate distribution (frequency of different climates)
    plt.figure(figsize=(10, 6))
    df['Climate'].value_counts().plot(kind='bar', color='lightgreen', edgecolor='black')
    plt.title('Climate Distribution for All Cities')
    plt.xlabel('Climate')
    plt.ylabel('Frequency')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(os.path.join(PLOTS_DIR, f'{date}_climate_distribution.png'))
    plt.close()

def plot_historical_data(df):
    # Ensure the DataFrame contains the necessary columns
    required_columns = ['CityID', 'Date', 'AvgTemp', 'MaxTemp', 'MinTemp', 'AvgHumidity', 'CityName']
    for column in required_columns:
        if column not in df.columns:
            raise ValueError(f"Missing required column: {column}")

    # Create a new directory if it doesn't exist
    # os.makedirs(PLOTS_DIR, exist_ok=True)

    # Convert 'Date' to datetime if not already
    # df['Serial'] = range(1, len(df) + 1)
    df.loc[:, 'Serial'] = range(1, len(df) + 1)

    # Create a figure for the plots
    plt.figure(figsize=(15, 12))

    # Plot Average Temperature vs Time
    for city in df['CityName'].unique():
        city_data = df[df['CityName'] == city]
        plt.plot(city_data['Serial'], city_data['AvgTemp'], marker='o', label=city)
    plt.title('Average Temperature Over Time')
    plt.xlabel('Serial')
    plt.ylabel('Average Temperature (°C)')
    plt.xticks(rotation=45)
    plt.legend()
    plt.grid()
    plt.tight_layout()
    plt.savefig(os.path.join(PLOTS_DIR, 'History', 'avg_temp.png'))
    plt.close()

    # Plot Minimum Temperature vs Time
    for city in df['CityName'].unique():
        city_data = df[df['CityName'] == city]
        plt.plot(city_data['Serial'], city_data['MinTemp'], marker='o', label=city)
    plt.title('Minimum Temperature Over Time')
    plt.xlabel('Serial')
    plt.ylabel('Minimum Temperature (°C)')
    plt.xticks(rotation=45)
    plt.legend()
    plt.grid()
    plt.tight_layout()
    plt.savefig(os.path.join(PLOTS_DIR, 'History', 'min_temp.png'))
    plt.close()

    # Plot Maximum Temperature vs Time
    for city in df['CityName'].unique():
        city_data = df[df['CityName'] == city]
        plt.plot(city_data['Serial'], city_data['MaxTemp'], marker='o', label=city)
    plt.title('Maximum Temperature Over Time')
    plt.xlabel('Serial')
    plt.ylabel('Maximum Temperature (°C)')
    plt.xticks(rotation=45)
    plt.legend()
    plt.grid()
    plt.tight_layout()
    plt.savefig(os.path.join(PLOTS_DIR, 'History', 'max_temp.png'))
    plt.close()

    # Plot Average Humidity vs Time
    for city in df['CityName'].unique():
        city_data = df[df['CityName'] == city]
        plt.plot(city_data['Serial'], city_data['AvgHumidity'], marker='o', label=city)
    plt.title('Average Humidity Over Time')
    plt.xlabel('Serial')
    plt.ylabel('Average Humidity (%)')
    plt.xticks(rotation=45)
    plt.legend()
    plt.grid()
    plt.tight_layout()
    plt.savefig(os.path.join(PLOTS_DIR, 'History', 'avg_humidity.png'))
    plt.close()

# ater a new day begins, the data from timestamp_average is moved to Daily_Averages and the cities table is updated with the new data. timestamp_average is then cleared to start fresh for the new day                       
def daychange():
    clear_daily_data_query = "TRUNCATE TABLE timestamp_average"
    today_date = datetime.now().strftime('%Y-%m-%d')

    # Insert data from cities table into Daily_Averages with today's date
    update_daily_average_query = """
    INSERT INTO Daily_Averages (CityID, Date, AvgTemp, MinTemp, MaxTemp, AvgHumidity, MostFreqClimate)
    SELECT CityID, %s, AvgTemp, MinTemp, MaxTemp, AvgHumidity, MostFreqClimate
    FROM cities
    """
    
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    if connection.is_connected():
        cursor.execute("SELECT * FROM timestamp_average")
        todays_data = cursor.fetchall()
        cursor.execute("SELECT CityID, CityName FROM cities")
        cities_data = cursor.fetchall()
        cursor.execute("SELECT * FROM Daily_Averages")
        daily_data = cursor.fetchall()
        cursor.execute(update_daily_average_query, (today_date,))
        cursor.execute(clear_daily_data_query)
        connection.commit()
        
        update_initial_query = """
        UPDATE cities
        SET 
            T1 = AvgTemp,
            AvgTemp = NULL,
            MinTemp = NULL,
            MaxTemp = NULL,
            AvgHumidity = NULL,
            MostFreqClimate = NULL;
        """

        # Step 2: Update AvgTempLast7Days using last 7 entries from Daily_Averages
        update_avg_temp_last_7_days_query = """
UPDATE cities c
JOIN (
    SELECT CityID, AVG(AvgTemp) AS AvgTempLast7Days
    FROM Daily_Averages
    WHERE Date IN (
        SELECT Date
        FROM (
            SELECT Date, CityID
            FROM Daily_Averages
            WHERE CityID IN (SELECT DISTINCT CityID FROM cities)
            ORDER BY Date DESC
            LIMIT 7
        ) AS temp_dates
    )
    GROUP BY CityID
) da ON c.CityID = da.CityID
SET c.AvgTempLast7Days = da.AvgTempLast7Days;
        """
        cursor.execute(update_initial_query)
        cursor.execute(update_avg_temp_last_7_days_query)

        connection.commit()

    cursor.close()
    df = pd.DataFrame(todays_data, columns=["CityID", "TimeStamp", "Temp", "Feels_Like", "Humidity", "Climate"])
    df['Temp'] = df['Temp'].astype(float)
    df['Feels_Like'] = df['Feels_Like'].astype(float)
    df['Humidity'] = df['Humidity'].astype(float)
    
    cities_to_name = {city[0]: str(city[1]) for city in cities_data}
    df['CityName'] = df['CityID'].map(cities_to_name)
    
    daily_data = pd.DataFrame(daily_data, columns=["CityID", "Date", "AvgTemp", "MinTemp", "MaxTemp", "AvgHumidity", "MostFreqClimate"])
    daily_data['AvgTemp'] = daily_data['AvgTemp'].astype(float)
    daily_data['MinTemp'] = daily_data['MinTemp'].astype(float)
    daily_data['MaxTemp'] = daily_data['MaxTemp'].astype(float)
    daily_data['AvgHumidity'] = daily_data['AvgHumidity'].astype(float)
    daily_data['CityName'] = daily_data['CityID'].map(cities_to_name)
    
    os.makedirs(PLOTS_DIR, exist_ok=True)
    os.makedirs(os.path.join(PLOTS_DIR, "History"), exist_ok=True)
    for city in df['CityName'].unique():
        os.makedirs(os.path.join(PLOTS_DIR, city), exist_ok=True)
    plot_average_temp_humidity(df)
    plot_daily_weather(df)
    plot_temperature_distribution(df)
    plot_climate_distribution(df)
    plot_historical_data(daily_data)

# generates data summary, stores plots and sends alerts
def interpret(data):
    for city in data:
        CityID, CityName, Longitude, Latitude, AvgTempLast7Days, T1, AvgTemp, MinTemp, MaxTemp, AvgHumidity, MostFreqClimate = city
        
        summary = (f"City: {CityName}\n"
                   f"Average Temperature Last 7 Days: {AvgTempLast7Days}°C\n"
                   f"Current Temperature (T1): {T1}°C\n"
                   f"Avg Temperature: {AvgTemp}°C\n"
                   f"Min Temperature: {MinTemp}°C\n"
                   f"Max Temperature: {MaxTemp}°C\n"
                   f"Average Humidity: {AvgHumidity}%\n"
                   f"Most Frequent Climate: {MostFreqClimate}\n")

        print(summary)
        
        alert_message = ""
        if AvgTemp > HIGH_TEMP_THRESHOLD:
            alert_message += (f"Alert: High temperature in {CityName}! "
                             f"Current temperature is {AvgTemp}°C.\n")
        
        elif AvgTemp < LOW_TEMP_THRESHOLD:
            alert_message += (f"Alert: Low temperature in {CityName}! "
                             f"Current temperature is {AvgTemp}°C.\n")
        
        if AvgHumidity > HIGH_HUMIDITY_THRESHOLD:
            alert_message += (f"Alert: High humidity in {CityName}! "
                             f"Current humidity is {AvgHumidity}%.\n")
        
        elif AvgHumidity < LOW_HUMIDITY_THRESHOLD:
            alert_message += (f"Alert: Low humidity in {CityName}! "
                             f"Current humidity is {AvgHumidity}%.\n")
            
        print(alert_message)  # Replace with email or other alert sending logic

# gets weather data for all cities, processes it and updates the database
def new_interval():
    result = {}
    print("Getting New Results")
    for city in CITIES:
        data = get_weather_data(city)
        if data:
            processed_data = process_weather_data(data)
            result[city] = processed_data

    print("Updating Database for fetched results at timestamp: ", datetime.now())
    new_stats = update_database(result)
    interpret(new_stats)

    
if __name__ == "__main__":
    last_date = date.today()
    while True:
        current_date = date.today()
        if current_date != last_date:
            daychange()
            last_date = current_date
        new_interval()
        time.sleep(UPDATE_INTERVAL)
    
    # For testing day change, uncomment the below code and remove primary key from Daily_Averages table and add it back after testing
    # while(True):
    #     for i in range(2):
    #         new_interval()
    #         time.sleep(UPDATE_INTERVAL)
    #     print("##################################################")
    #     print("Day Changing")
    #     daychange()
    #     print("Day Changed")
    #     print("##################################################")
        
        
        
        
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