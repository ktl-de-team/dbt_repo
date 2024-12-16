#!/bin/bash
# dbt_setup.sh

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "Python 3 is required but not installed."
    exit 1
fi

# Function to print step header
print_step() {
    echo "================================================"
    echo "$1"
    echo "================================================"
}

# Check directory structure
print_step "Checking directory structure..."
python3 dbt_rawvault_manager.py check
if [ $? -ne 0 ]; then
    print_step "Creating missing directories..."
    python3 dbt_rawvault_manager.py create
fi

# Generate SQL files
print_step "Generating SQL files..."
python3 dbt_rawvault_manager.py generate

# Check final status
if [ $? -eq 0 ]; then
    print_step "Setup completed successfully!"
    exit 0
else
    print_step "Setup completed with errors. Please check the logs."
    exit 1
fi