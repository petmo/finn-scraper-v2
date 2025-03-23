#!/bin/bash
set -e

echo "Starting Finn.no scraper sequence at $(date)"

# Run finn code scraper
echo "Step 1: Running finn code scraper..."
python update_finn_codes.py active
echo "Finn code scraper completed"

# Run inactive checker
echo "Step 2: Marking inactive listings..."
python update_finn_codes.py inactive
echo "Inactive marking completed"

# Run property scraper
echo "Step 3: Processing properties..."
python process_properties.py
echo "Property processing completed"

echo "Full scraping sequence completed at $(date)"