# Finn.no Property Scraper

A Python application for scraping property listing data from Finn.no.

## Features

- Scrape property listings from Finn.no
- Store data in various backends:
  - SQLite database
  - CSV files
  - Supabase cloud database
- Geocoding of property addresses
- Extensible architecture with service layer and storage abstraction

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/finn-property-scraper.git
   cd finn-property-scraper
   ```

2. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

3. Configure the application by editing `config/config.yaml`

## Configuration

The application is configured using the `config/config.yaml` file. Key settings include:

- `backend`: Choose between `sqlite`, `csv`, or `supabase`
- `base_url`: The search page URL for Finn.no 
- `max_page`: Maximum number of pages to scrape
- Backend-specific settings (SQLite, CSV, or Supabase)

### Supabase Configuration

To use Supabase as a backend:

1. Create a Supabase account at [supabase.com](https://supabase.com)
2. Create a new project
3. Create the following tables:

#### Finn Codes Table

```sql
CREATE TABLE public.finn_codes (
    finn_code TEXT PRIMARY KEY,
    fetched_at TIMESTAMP,
    scrape_status TEXT
);
```

#### Properties Table

```sql
CREATE TABLE public.properties (
    finn_code TEXT PRIMARY KEY,
    title TEXT,
    address TEXT,
    asking_price TEXT,
    total_price TEXT,
    costs TEXT,
    joint_debt TEXT,
    monthly_fee TEXT,
    property_type TEXT,
    ownership TEXT,
    bedrooms TEXT,
    internal_area TEXT,
    usable_area TEXT,
    external_usable_area TEXT,
    floor TEXT,
    build_year TEXT,
    rooms TEXT,
    local_area TEXT,
    area_name TEXT,
    image_0 TEXT,
    image_1 TEXT,
    image_2 TEXT,
    latitude TEXT,
    longitude TEXT,
    scrape_status TEXT
);
```

4. Update the `config.yaml` file with your Supabase URL and API key:
   ```yaml
   backend: "supabase"
   supabase:
     url: "https://yourprojectid.supabase.co"
     key: "your-api-key"
   ```

## Usage

The application consists of two main scripts:

### 1. Scrape Finn Codes

```
python run_finn_code_scraper.py [options]
```

Options:
- `--backend {sqlite,csv,supabase}`: Override backend type from configuration
- `--drop-tables`: Drop existing finn_codes table before scraping
- `--export-csv PATH`: Export finn codes to a CSV file at the specified path

### 2. Scrape Property Details

```
python run_property_scraper.py [options]
```

Options:
- `--backend {sqlite,csv,supabase}`: Override backend type from configuration
- `--drop-tables`: Drop existing properties table before scraping
- `--export-csv PATH`: Export properties to a CSV file at the specified path
- `--limit N`: Limit number of properties to scrape
- `--scrape-all`: Scrape all finn codes, not just pending ones

## Example Workflow

1. Scrape finn codes from search pages:
   ```
   python run_finn_code_scraper.py
   ```

2. Scrape property details for each finn code:
   ```
   python run_property_scraper.py
   ```

3. Export data to CSV for analysis:
   ```
   python run_property_scraper.py --export-csv data/properties.csv
   ```

## Extending the Application

### Adding a New Backend

1. Create a new class that inherits from `StorageBackend` in `scraper/storage/base.py`
2. Implement all the required methods
3. Register your backend in `scraper/storage/factory.py`

### Adding New Parsers

Add new parser classes in the `scraper/parsers` directory.

## License

MIT