import requests
import time
from datetime import datetime, timezone
from collections import defaultdict
from datetime import date
import matplotlib.pyplot as plt
import mysql.connector
from mysql.connector import Error

API_KEY = '7c5de860ec41f4a02a0a246cad36b91e'
CITIES = ['Delhi', 'Mumbai', 'Chennai', 'Bangalore', 'Kolkata', 'Hyderabad']
BASE_URL = 'http://api.openweathermap.org/data/2.5/weather?q={}&appid={}&units=metric'
UPDATE_INTERVAL = 200  # 5 minutes

# A dictionary to store daily weather data for rollups
daily_data = defaultdict(lambda: {'temps': [], 'conditions': []})

def convert_temperature(temp_kelvin, unit="Celsius"):
    if unit == "Celsius":
        return temp_kelvin - 273.15
    elif unit == "Fahrenheit":
        return (temp_kelvin - 273.15) * 9/5 + 32
    else:
        return temp_kelvin  # Default is Kelvin

def get_weather_data(city):
    response = requests.get(BASE_URL.format(city, API_KEY))
    if response.status_code == 200:
        return response.json()
    else:
        print(
            f"Failed to fetch data for {city}: {response.status_code}, {response.text}")
        return None


def process_weather_data(data):
    if data:
        weather_main = data['weather'][0]['main']
        temp = data['main']['temp']  # already in Celsius due to `units=metric`
        feels_like = data['main']['feels_like']
        timestamp = data['dt']
        return {
            'temp': temp,
            'feels_like': feels_like,
            'weather_main': weather_main,
            'timestamp': timestamp
        }
    return None

# Store data in daily summaries
def update_daily_rollups(city, processed_data):
    # Use fromtimestamp with UTC timezone
    current_date = datetime.fromtimestamp(processed_data['timestamp'], tz=timezone.utc).strftime('%Y-%m-%d')
    daily_data[city]['temps'].append(processed_data['temp'])
    daily_data[city]['conditions'].append(processed_data['weather_main'])
    
    # Add a new list to store dates for the first time
    if 'dates' not in daily_data[city]:
        daily_data[city]['dates'] = []
        
    # Append the date if it's not already in the list
    if current_date not in daily_data[city]['dates']:
        daily_data[city]['dates'].append(current_date)


# Aggregates: daily summary calculations


def calculate_daily_summary(city, date):
    temps = daily_data[city]['temps']
    conditions = daily_data[city]['conditions']
    avg_temp = sum(temps) / len(temps)
    max_temp = max(temps)
    min_temp = min(temps)
    # most frequent weather condition
    dominant_condition = max(set(conditions), key=conditions.count)
    return {
        'city': city,
        'date': date,
        'avg_temp': avg_temp,
        'max_temp': max_temp,
        'min_temp': min_temp,
        'dominant_condition': dominant_condition
    }

# Alerting mechanism


def check_alerts(city, processed_data, threshold):
    if processed_data['temp'] > threshold:
        print(
            f"ALERT: {city} has exceeded the temperature threshold of {threshold}°C. Current temperature: {processed_data['temp']}°C")

def plot_temperature_trend(city):
    dates = list(daily_data[city]['dates'])
    temps = list(daily_data[city]['temps'])
    
    plt.plot(dates, temps)
    plt.title(f'Temperature trend for {city}')
    plt.xlabel('Date')
    plt.ylabel('Temperature (°C)')
    plt.show()
    
# Database connection details
MYSQL_HOST = 'localhost'  # Change this to your MySQL server hostname
MYSQL_USER = 'weather_user'       # Your MySQL username
MYSQL_PASSWORD = 'new_password'  # Your MySQL password
MYSQL_DATABASE = 'weather_data'  # The database you created

# Function to connect to the MySQL database and store the summary
def store_summary_in_db(summary):
    try:
        # Establish connection to the MySQL database
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE
        )
        if conn.is_connected():
            print(f"Connected to MySQL database {MYSQL_DATABASE}")

        cursor = conn.cursor()

        # Create table if it doesn't exist
        cursor.execute('''CREATE TABLE IF NOT EXISTS weather_summary (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            city VARCHAR(255),
                            date DATE,
                            avg_temp FLOAT,
                            max_temp FLOAT,
                            min_temp FLOAT,
                            dominant_condition VARCHAR(255)
                          )''')

        # Insert weather summary into the table
        query = '''INSERT INTO weather_summary (city, date, avg_temp, max_temp, min_temp, dominant_condition)
                   VALUES (%s, %s, %s, %s, %s, %s)'''
        values = (summary['city'], summary['date'], summary['avg_temp'], 
                  summary['max_temp'], summary['min_temp'], summary['dominant_condition'])
        
        cursor.execute(query, values)
        conn.commit()
        print(f"Weather summary for {summary['city']} on {summary['date']} stored in database.")

    except Error as e:
        print(f"Error: {e}")
    
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
            print("MySQL connection closed.")

# Example: Call store_summary_in_db after generating daily summaries
def generate_daily_summary():
    today = date.today().strftime('%Y-%m-%d')
    for city in CITIES:
        if today in daily_data[city]['dates']:
            summary = calculate_daily_summary(city, today)
            print(f"Summary for {city} on {today}: {summary}")
            store_summary_in_db(summary)
        else:
            print(f"No data for {city} today.")


for _ in range(10):
    for city in CITIES:
        data = get_weather_data(city)
        if data:
            processed_data = process_weather_data(data)
            print(f"{city}: {processed_data}")
            update_daily_rollups(city, processed_data)
            check_alerts(city, processed_data, 35)
    time.sleep(UPDATE_INTERVAL)

generate_daily_summary()