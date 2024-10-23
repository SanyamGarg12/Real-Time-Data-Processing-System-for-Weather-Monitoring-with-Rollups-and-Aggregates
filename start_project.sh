#!/bin/bash

# Define the Python interpreter (adjust if necessary)
PYTHON="python"  # Use 'python' if Python 3 is the default

# Run database_creation.py
echo "Running database_creation.py..."
$PYTHON database_creation.py

# Check if the previous command was successful
if [ $? -ne 0 ]; then
    echo "Error occurred while running database_creation.py"
    exit 1
fi

# Run add_cities.py
echo "Running add_cities.py..."
$PYTHON add_cities.py

# Check if the previous command was successful
if [ $? -ne 0 ]; then
    echo "Error occurred while running add_cities.py"
    exit 1
fi

# Run main.py
echo "Running main.py..."
$PYTHON main.py

# Check if the previous command was successful
if [ $? -ne 0 ]; then
    echo "Error occurred while running main.py"
    exit 1
fi

echo "All scripts executed successfully."
