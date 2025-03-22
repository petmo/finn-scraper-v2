# finn_scraper/scraper/db_manager.py
import sqlite3
import logging
import csv

logger = logging.getLogger(__name__)

def create_connection(db_name):
    """Creates a database connection to a SQLite database."""
    conn = None
    try:
        conn = sqlite3.connect(db_name)
        logger.info(f"Connected to database: {db_name}")
    except sqlite3.Error as e:
        logger.error(f"Database connection error: {e}")
    return conn

def _drop_table(conn, table_name):
    """Drops a specified table if it exists."""
    try:
        cursor = conn.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.commit()
        logger.info(f"Existing table '{table_name}' dropped.")
    except sqlite3.Error as e:
        logger.error(f"Database error dropping table '{table_name}': {e}")

def drop_finn_codes_table(conn):
    """Drops only the 'finn_codes' table if it exists."""
    _drop_table(conn, 'finn_codes')

def drop_properties_table(conn):
    """Drops only the 'properties' table if it exists."""
    _drop_table(conn, 'properties')

def create_table_finn_codes(conn):
    """Creates the finn_codes table in the database if it doesn't exist."""
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS finn_codes (
                finn_code TEXT PRIMARY KEY,
                fetched_at TEXT,
                scrape_status TEXT DEFAULT 'pending'
            )
        """)
        conn.commit()
        logger.info("Table 'finn_codes' created or already exists.")
    except sqlite3.Error as e:
        logger.error(f"Database error creating finn_codes table: {e}")

def create_table_properties(conn):
    """Creates the properties table in the database if it doesn't exist."""
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS properties (
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
                image_1 TEXT,
                image_2 TEXT,
                image_3 TEXT,
                scrape_status TEXT DEFAULT 'pending'
            )
        """)
        conn.commit()
        logger.info("Table 'properties' created or already exists.")
    except sqlite3.Error as e:
        logger.error(f"Database error creating properties table: {e}")

def save_finn_codes(conn, finn_codes_data):
    """Saves fetched finn codes to the database."""
    sql = """
        INSERT OR IGNORE INTO finn_codes (finn_code, fetched_at) VALUES (?, ?)
    """
    try:
        cursor = conn.cursor()
        cursor.executemany(sql, [(item['finn_code'], item['fetched_at']) for item in finn_codes_data])
        conn.commit()
        logger.info(f"Saved {len(finn_codes_data)} Finn codes to database.")
    except sqlite3.Error as e:
        logger.error(f"Database error saving finn codes: {e}")

def fetch_finn_codes_from_db(conn, select_all=False):
    """Fetches Finn codes from the database that have not been scraped yet."""
    if not select_all:
        query ="SELECT finn_code FROM finn_codes WHERE scrape_status = 'pending'"
    else:
        query = "SELECT finn_code FROM finn_codes"

    try:
        cursor = conn.cursor()
        cursor.execute(query)
        finn_codes = cursor.fetchall()
        logger.info(f"Fetched {len(finn_codes)} Finn codes from database for scraping.")
        return finn_codes
    except sqlite3.Error as e:
        logger.error(f"Database error fetching finn codes: {e}")
        return []

def save_property_data(conn, property_data):
    """
    Saves property data to the database.
    Maps the parsed property data keys to the database columns:
      - 'asking_price' -> asking_price
      - 'ownership' -> ownership
      - 'internal_area' -> internal_area
      - 'build_year' -> build_year
    """
    sql = """
        INSERT OR REPLACE INTO properties (
            finn_code, title, address, asking_price, total_price,
            costs, joint_debt, monthly_fee, property_type, ownership,
            bedrooms, internal_area, usable_area, external_usable_area,
            floor, build_year, rooms, local_area, image_1, image_2, image_3, latitude, longitude, scrape_status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    try:
        cursor = conn.cursor()
        cursor.execute(sql, (
            property_data.get('finn_code'),
            property_data.get('title'),
            property_data.get('address'),
            property_data.get('asking_price'),  # mapped to asking_price
            property_data.get('total_price'),
            property_data.get('costs'),
            property_data.get('joint_debt'),
            property_data.get('monthly_fee'),
            property_data.get('property_type'),
            property_data.get('ownership'),      # mapped to ownership
            property_data.get('bedrooms'),
            property_data.get('internal_area'), # mapped to internal_area
            property_data.get('usable_area'),
            property_data.get('external_usable_area'),
            property_data.get('floor'),
            property_data.get('build_year'),           # mapped to build_year
            property_data.get('rooms'),
            property_data.get('local_area'),
            property_data.get('image_1'),
            property_data.get('image_2'),
            property_data.get('image_3'),
            property_data.get('latitude'),
            property_data.get('longitude'),
            property_data.get('scrape_status', 'success'),
        ))
        conn.commit()
        logger.info(f"Saved property data for Finn code: {property_data.get('finn_code')}")
    except sqlite3.Error as e:
        logger.error(f"Database error saving property data: {e}")

def update_finn_code_status(conn, finn_code, status):
    """Updates the scrape status of a finn_code in the finn_codes table."""
    sql = """
        UPDATE finn_codes SET scrape_status = ? WHERE finn_code = ?
    """
    try:
        cursor = conn.cursor()
        cursor.execute(sql, (status, finn_code))
        conn.commit()
        logger.info(f"Updated scrape status for Finn code {finn_code} to: {status}")
    except sqlite3.Error as e:
        logger.error(f"Database error updating scrape status for Finn code {finn_code}: {e}")

def export_properties_to_csv(conn, csv_name="finn_properties.csv"):
    """Exports data from the properties table to a CSV file."""
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM properties")
        rows = cursor.fetchall()
        if rows:
            with open(csv_name, 'w', newline='', encoding='utf-8') as csvfile:
                csv_writer = csv.writer(csvfile)
                csv_writer.writerow([description[0] for description in cursor.description])
                csv_writer.writerows(rows)
            logger.info(f"Property data exported to CSV file: {csv_name}")
        else:
            logger.info("No property data to export from the database.")
    except sqlite3.Error as e:
        logger.error(f"Database error exporting property data to CSV: {e}")

def export_finn_codes_to_csv(conn, csv_name="finn_codes.csv"):
    """Exports data from the finn_codes table to a CSV file."""
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM finn_codes")
        rows = cursor.fetchall()
        if rows:
            with open(csv_name, 'w', newline='', encoding='utf-8') as csvfile:
                csv_writer = csv.writer(csvfile)
                csv_writer.writerow([description[0] for description in cursor.description])
                csv_writer.writerows(rows)
            logger.info(f"Finn codes exported to CSV file: {csv_name}")
        else:
            logger.info("No finn codes to export from the database.")
    except sqlite3.Error as e:
        logger.error(f"Database error exporting finn codes to CSV: {e}")
