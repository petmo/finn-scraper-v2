# run_finn_code_scraper.py
import logging
from scraper import core, db_manager, utils
import datetime

def main():
    config = utils.load_config()
    utils.setup_logging()
    logger = logging.getLogger(__name__)

    logger.info("Starting Finn.no Finn Code scraper")

    conn = db_manager.create_connection(config['database_name'])
    if conn:
        db_manager.drop_finn_codes_table(conn) # Drop only finn_codes table
        db_manager.create_table_finn_codes(conn)

        finn_codes = core.fetch_finn_codes(config)
        finn_codes_data = []
        for finn_code in finn_codes:
            finn_codes_data.append({'finn_code': finn_code, 'fetched_at': datetime.datetime.now().isoformat()}) # Add timestamp
        db_manager.save_finn_codes(conn, finn_codes_data)

        db_manager.export_finn_codes_to_csv(conn, config['finn_codes_csv_export_name'])
        conn.close()
        logger.info("Database connection closed.")
    else:
        logger.error("Failed to connect to database. Scraper aborted.")

    logger.info("Finn.no Finn Code scraper finished.")

if __name__ == "__main__":
    main()