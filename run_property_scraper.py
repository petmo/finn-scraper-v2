# run_property_scraper.py
import logging

import pandas as pd

from scraper import core, db_manager, utils

logging.basicConfig(level=logging.INFO) # Enable logging INFO level for more detailed output


N_SCRAPES = None

def main():
    config = utils.load_config()
    utils.setup_logging()
    logger = logging.getLogger(__name__)

    logger.info("Starting Finn.no Property Details scraper")

    conn = db_manager.create_connection(config['database_name'])
    if conn:
        db_manager.drop_properties_table(conn) # Drop only properties table
        db_manager.create_table_properties(conn)

        finn_codes_to_scrape = db_manager.fetch_finn_codes_from_db(conn, select_all=True)
        properties_data = []
        for i, finn_code_data in enumerate(finn_codes_to_scrape):
            finn_code = finn_code_data[0] # Assuming finn_code is the first element in the tuple
            property_data = core.scrape_property_details(finn_code, config)
            if property_data:
                property_data['finn_code'] = finn_code # Ensure finn_code is in property data

                # Geocode address and add lat/long
                address_to_geocode = property_data.get('address')
                if address_to_geocode:
                    geocode_result = core.geocode_address(address_to_geocode)
                    if geocode_result:
                        property_data['latitude'] = geocode_result['latitude']
                        property_data['longitude'] = geocode_result['longitude']
                        logger.info(f"Geocoded address for Finn code {finn_code}: Lat={property_data['latitude']}, Long={property_data['longitude']}")
                    else:
                        logger.warning(f"Geocoding failed for address: {address_to_geocode} (Finn code: {finn_code})")
                else:
                    logger.warning(f"No address found for Finn code {finn_code}, cannot geocode.")


                db_manager.save_property_data(conn, property_data)
                db_manager.update_finn_code_status(conn, finn_code, "success")
                properties_data.append(property_data)
            else:
                db_manager.update_finn_code_status(conn, finn_code, "failed")

            if N_SCRAPES is not None:
                if i == N_SCRAPES:
                    break

        df_all = pd.DataFrame(properties_data)
        df_all.to_csv(config['csv_export_name'], index=False)
        #db_manager.export_properties_to_csv(conn, config['csv_export_name'])
        conn.close()
        logger.info("Database connection closed.")
    else:
        logger.error("Failed to connect to database. Scraper aborted.")

    logger.info("Finn.no Property Details scraper finished.")


def geocode_existing_properties():
    config = utils.load_config()
    utils.setup_logging()
    logger = logging.getLogger(__name__)

    logger.info("Starting Geocoding for Existing Properties")

    conn = db_manager.create_connection(config['database_name'])
    if conn:
        properties_to_geocode = db_manager.fetch_properties_without_location(conn)
        if not properties_to_geocode:
            logger.info("No properties found without location data to geocode.")
            return

        for property_record in properties_to_geocode:
            finn_code = property_record[0] # Assuming finn_code is the first element
            address_to_geocode = property_record[2] # Assuming address is the third element (after finn_code, scrape_status)

            if address_to_geocode:
                geocode_result = core.geocode_address(address_to_geocode)
                if geocode_result:
                    latitude = geocode_result['latitude']
                    longitude = geocode_result['longitude']
                    db_manager.update_property_location(conn, finn_code, latitude, longitude)
                    logger.info(f"Geocoded Finn code {finn_code}: Address='{address_to_geocode}', Lat={latitude}, Long={longitude}")
                else:
                    logger.warning(f"Geocoding failed for address: '{address_to_geocode}' (Finn code: {finn_code})")
            else:
                logger.warning(f"No address found in database for Finn code {finn_code}, cannot geocode.")

        conn.close()
        logger.info("Database connection closed after geocoding.")
    else:
        logger.error("Failed to connect to database for geocoding. Geocoding aborted.")

    logger.info("Geocoding for Existing Properties finished.")


if __name__ == "__main__":
    main()
    #geocode_existing_properties() # Automatically geocode after scraping in the same run