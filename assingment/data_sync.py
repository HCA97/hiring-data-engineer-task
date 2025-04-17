import os
import logging
import time
import argparse
import clickhouse_connect
from clickhouse_connect.driver.client import Client

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)

INIT_SQL_FILE = os.getenv('INIT_SQL_FILE', 'clickhouse_init.sql')
CLICKHOUSE_CONFIG = {
    'host': os.getenv('CLICKHOUSE_HOST', 'localhost'),
    'port': 8123,
    'user': os.getenv('CLICKHOUSE_USER', 'default'),
    'password': os.getenv('CLICKHOUSE_PASSWORD', 'value'),
    'database': os.getenv('CLICKHOUSE_DATABASE', 'default'),
}

PG_USER = os.getenv('PG_USER', 'postgres')
# This is postgres connection but we are using inside the Clikchouse, 
# so we need to set the host to the postgres container name
PG_PASSWORD = os.getenv('PG_PASSWORD', 'postgres')
PG_HOST = os.getenv('PG_HOST', 'postgres')
PG_PORT = os.getenv('PG_PORT', '5432')
PG_DATABASE = os.getenv('PG_DATABASE', 'postgres')
# Query configs
SLEEP_INTERVAL = int(os.getenv('SLEEP_INTERVAL', 600)) 
MAX_ROWS = int(os.getenv('MAX_ROWS', 1000000))
INSERT_QUERIES = {
    # small table just insert all the time (less than few thousand rows) otherwise so much headache
    "advertiser": """
        TRUNCATE TABLE default.advertiser;
        INSERT INTO default.advertiser 
        SELECT id, name, created_at, updated_at
        FROM postgresql('{PG_HOST}:{PG_PORT}', '{PG_DATABASE}', 'advertiser', {PG_USER}, {PG_PASSWORD});
    """,
    "campaign": """
        TRUNCATE TABLE default.campaign;
        INSERT INTO default.campaign
        SELECT id, name, bid, budget, start_date, end_date, advertiser_id, updated_at, created_at
        FROM postgresql('{PG_HOST}:{PG_PORT}', '{PG_DATABASE}', 'campaign', {PG_USER}, {PG_PASSWORD});
    """,
    # the `created_at` is not `now()` but some random date, pff
    "clicks": """
        INSERT INTO default.clicks
        SELECT id, campaign_id, created_at
        FROM postgresql('{PG_HOST}:{PG_PORT}', '{PG_DATABASE}', 'clicks', {PG_USER}, {PG_PASSWORD})
        WHERE  id > (SELECT MAX(id) FROM default.clicks)
        ORDER BY id ASC
        LIMIT {MAX_ROWS};
    """,
    "impressions": """
        INSERT INTO default.impressions
        SELECT id, campaign_id, created_at      
        FROM postgresql('{PG_HOST}:{PG_PORT}', '{PG_DATABASE}', 'impressions', {PG_USER}, {PG_PASSWORD})
        WHERE  id > (SELECT MAX(id) FROM default.impressions)
        ORDER BY id ASC
        LIMIT {MAX_ROWS};
    """,
}

def sync_data(client: Client, table_name: str, queries: str) -> bool:
    try:
        t0 = time.time()
        for query in queries.split(";"):
            if query.strip():
                insert_query = query.format(
                    PG_HOST=PG_HOST,
                    PG_PORT=PG_PORT,
                    PG_DATABASE=PG_DATABASE,
                    PG_USER=PG_USER,
                    PG_PASSWORD=PG_PASSWORD,
                    MAX_ROWS=MAX_ROWS,
                )
                # Last query is the insert query
                res = client.command(insert_query)
        t1 = time.time()
        logging.info(f"[{table_name.upper():<11}] Data sync completed successfully | {res.written_rows:>9} rows inserted | Time taken: {t1 - t0:>6.2f} seconds")
        return True
    except Exception as e:
        logging.error(f"[{table_name.upper()}] Error during sync: {str(e)}")
        return False

def run_init_query(client: Client) -> int:
    with open(INIT_SQL_FILE, "r") as file:
        init_queries = file.read().split(";")
    for query in init_queries:
        if query.strip():
            try:
                client.query(query)
            except Exception as e:
                logging.error(f"Error executing init query: {str(e)}")
                return False
    return True
                

def run_sync_job(client: Client):
    """Wrapper function for the scheduler."""
    logging.info("Starting scheduled sync job")

    # Sync each table
    for table_name, query in INSERT_QUERIES.items():
        if not sync_data(client, table_name, query):
            logging.error(f"Failed to sync {table_name}")
            continue
    logging.info("Finished scheduled sync job")

def main():
    """Main function to set up and run the scheduler."""

    parser = argparse.ArgumentParser(description="ClickHouse Sync Scheduler")
    parser.add_argument("--run-once", action="store_true", help="Run the sync job once and exit")
    args = parser.parse_args()


    logging.info("Initializing ClickHouse sync scheduler")
    client = clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)
    if not client:
        logging.error("Failed to connect to ClickHouse")
        raise Exception("Failed to connect to ClickHouse")
    
    logging.info("Connected to ClickHouse successfully")

    # init query
    if not run_init_query(client):
        logging.error("Failed to run init query")
        raise Exception("Failed to run init query")

    # Run once at startup
    while True:
        try:
            run_sync_job(client)
            if args.run_once:
                break
            time.sleep(SLEEP_INTERVAL)
        except KeyboardInterrupt:
            logging.info("Scheduler interrupted by user")
            break
        except Exception as e:
            logging.error(f"Error during initial sync: {str(e)}")
            raise e


if __name__ == "__main__":
    main()