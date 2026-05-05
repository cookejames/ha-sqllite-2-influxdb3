import sqlite3
import json
from datetime import datetime, timezone
from influxdb_client_3 import InfluxDBClient3, Point
from dotenv import load_dotenv
import logging
import os

# Load environment variables
load_dotenv()

# Setup logging
DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"
logging_level = logging.DEBUG if DEBUG_MODE else logging.INFO
logging.basicConfig(level=logging_level, format='%(asctime)s - %(levelname)s - %(message)s')

# Retrieve configuration from environment variables
sqlite_db = os.getenv("SQLITE_DB")
influx_url = os.getenv("INFLUXDB_URL")
influx_token = os.getenv("INFLUXDB_TOKEN")
influx_database = os.getenv("INFLUXDB_DATABASE")  # replaces INFLUXDB_ORG + INFLUXDB_BUCKET

# Validate environment variables
# In dry run mode the InfluxDB connection vars are not required
required_env_vars = [sqlite_db, influx_url, influx_token, influx_database]
if any(v is None for v in required_env_vars):
    logging.error("One or more required environment variables are not set.")
    exit(1)

if DRY_RUN:
    logging.info("*** DRY RUN MODE — no data will be written to InfluxDB ***")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", 10000))

def connect_to_sqlite(db_path):
    try:
        # Connect to SQLite database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        logging.info("Successfully connected to SQLite")
        return conn, cursor
    except sqlite3.Error as e:
        logging.error(f"SQLite error: {e}")
        exit(1)

def connect_to_influxdb(url, token, database):
    try:
        # Connect to InfluxDB 3 - host should be just the hostname/base URL without trailing slash
        # InfluxDB 3 uses 'database' instead of 'org'+'bucket'
        client = InfluxDBClient3(host=url, token=token, database=database)
        logging.info("Successfully connected to InfluxDB 3")
        return client
    except Exception as e:
        logging.error(f"InfluxDB connection error: {e}")
        exit(1)

def get_oldest_influx_timestamp(client):
    try:
        # Discover all measurements in the database
        measurements_table = client.query(query="SHOW MEASUREMENTS", language="influxql")
        if measurements_table is None or measurements_table.num_rows == 0:
            logging.info("No measurements found in InfluxDB database.")
            return None

        # SHOW MEASUREMENTS returns a 'name' column
        measurement_names = [row.as_py() for row in measurements_table.column("name")]
        logging.info(f"Found {len(measurement_names)} measurement(s) in InfluxDB: {measurement_names}")

        oldest_ts = None
        for measurement in measurement_names:
            try:
                table = client.query(
                    query=f'SELECT * FROM "{measurement}" ORDER BY time ASC LIMIT 1',
                    language="influxql"
                )
                if table is not None and table.num_rows > 0:
                    time_col = table.column("time")
                    if time_col and len(time_col) > 0:
                        ts = time_col[0].as_py()
                        if ts is not None:
                            if oldest_ts is None or ts < oldest_ts:
                                oldest_ts = ts
                                logging.debug(f"New oldest timestamp {ts} from measurement '{measurement}'")
            except Exception as e:
                logging.warning(f"Could not query measurement '{measurement}': {e}")

        return oldest_ts.isoformat() if oldest_ts is not None else None

    except Exception as e:
        logging.error(f"Error querying InfluxDB for the oldest timestamp: {e}")
    return None

def format_timestamp(oldest_timestamp):
    try:
        # Convert ISO format timestamp to a string format compatible with SQLite
        if isinstance(oldest_timestamp, str):
            dt_obj = datetime.fromisoformat(oldest_timestamp.replace('Z', '+00:00'))
        else:
            dt_obj = oldest_timestamp
        return dt_obj.strftime("%Y-%m-%d %H:%M:%S")
    except ValueError as e:
        logging.error(f"Error parsing timestamp: {e}")
        exit(1)

def build_sqlite_query(formatted_timestamp):
    # Build the SQLite query with an optional timestamp filter
    base_query = """
    SELECT s.state, sm.entity_id, s.last_updated_ts, sa.shared_attrs
    FROM states s
    LEFT JOIN state_attributes sa ON sa.attributes_id = s.attributes_id
    JOIN states_meta sm ON sm.metadata_id = s.metadata_id
    """
    if formatted_timestamp:
        return f"{base_query} WHERE s.last_updated_ts < '{formatted_timestamp}' ORDER BY sm.entity_id ASC, s.last_updated_ts DESC"
    return f"{base_query} ORDER BY sm.entity_id ASC, s.last_updated_ts DESC"

def parse_attributes(shared_attrs):
    try:
        # Parse the shared attributes JSON
        return json.loads(shared_attrs)
    except (TypeError, json.JSONDecodeError) as e:
        logging.warning(f"Failed to parse attributes: {e}")
        return {}

_entity_exists_cache = {}

def check_entity_exists(client, measurement, domain, entity_id_short):
    if client is None:
        return True # For dry-run without client, assume exists
    cache_key = f"{measurement}:{domain}:{entity_id_short}"
    if cache_key in _entity_exists_cache:
        return _entity_exists_cache[cache_key]
    try:
        query = f'SELECT * FROM "{measurement}" WHERE "domain" = \'{domain}\' AND "entity_id" = \'{entity_id_short}\' LIMIT 1'
        table = client.query(query=query, language="influxql")
        exists = table is not None and table.num_rows > 0
        _entity_exists_cache[cache_key] = exists
        return exists
    except Exception as e:
        logging.warning(f"Error checking entity existence for {cache_key}: {e}")
        return False

def batch_insert_to_influx(client, rows):
    points = []
    for row in rows:
        state, entity_id, last_updated_ts, shared_attrs = row
        if state in ["unknown", "unavailable", "None"]:
            continue
        domain, _, entity_id_short = entity_id.partition('.')
        attributes_json = parse_attributes(shared_attrs)

        friendly_name = attributes_json.get('friendly_name', entity_id_short)
        unit_of_measurement = attributes_json.get('unit_of_measurement')

        if not unit_of_measurement:
            logging.debug(f"Skipping entity '{entity_id}' — no unit_of_measurement in attributes")
            continue

        if not check_entity_exists(client, unit_of_measurement, domain, entity_id_short):
            logging.debug(f"Skipping entity '{entity_id}' — no existing data in table '{unit_of_measurement}'")
            continue

        try:
            # Convert timestamp from Unix epoch to UTC-aware datetime object
            last_updated_dt = datetime.fromtimestamp(float(last_updated_ts), tz=timezone.utc)
            # Create an InfluxDB point with tags and fields
            point = Point(unit_of_measurement).tag("source", "HA").tag("domain", domain)
            point.tag("entity_id", entity_id_short).time(last_updated_dt)

            # Add the state value as either a numerical value or a string
            try:
                # Attempt to convert state to a float.
                val = float(state)
                point.field("value", val)
            except (ValueError, TypeError):
                # If it's not a number (ValueError) or not a string/number (TypeError),
                # we treat it as a string state.
                point.field("state", str(state))

            # Add additional attributes as fields, ensuring correct type
            for key, value in attributes_json.items():
                if key in ["id", "id_str", "update_available", "entity_id", "icon", "last_reset"]:
                    continue
                if value is None:
                    continue
                try:
                    if key in ["temperature", "humidity", "voc", "formaldehyd", "co2", "linkquality"]:
                        point.field(key, float(value))
                    elif isinstance(value, (int, float)) or (isinstance(value, str) and value.replace('.', '', 1).isdigit()):
                        point.field(key, float(value))
                    elif isinstance(value, str):
                        point.field(f"{key}_str", str(value))
                    else:
                        point.field(f"{key}", str(value))
                except Exception as e:
                    logging.warning(f"Skipping field '{key}' for entity '{entity_id}' with value '{value}' due to type conflict: {e}")

            points.append(point)
            logging.debug(
                f"Queued point → table='{unit_of_measurement}' "
                f"entity='{entity_id}' "
                f"value={state!r} "
                f"time={last_updated_dt.isoformat()}"
            )
            logging.debug(point)

        except ValueError as e:
            logging.warning(f"Error preparing InfluxDB point for entity {entity_id}: {e}, row: {row}")

    if points:
        if DRY_RUN:
            # Dry run: print each point's line protocol instead of writing
            for point in points:
                print(point.to_line_protocol())
        elif DEBUG_MODE:
            # Debug mode: write one point at a time for easier error tracing
            for point in points:
                try:
                    client.write(record=point)
                except Exception as e:
                    logging.error(f"Error writing point to InfluxDB: {e}. Point: {point}")
        else:
            try:
                client.write(record=points)
                logging.info(f"Successfully wrote {len(points)} points to InfluxDB")
            except Exception as e:
                logging.error(f"Error writing points to InfluxDB: {e}")
    else:
        logging.info("No points to write in this batch.")

def main():
    # Main execution flow
    conn, cursor = connect_to_sqlite(sqlite_db)
    client = connect_to_influxdb(influx_url, influx_token, influx_database)

    # Get the oldest timestamp from InfluxDB to determine how much data to process
    oldest_influx_timestamp = get_oldest_influx_timestamp(client)
    logging.info(f"Oldest InfluxDB timestamp: {oldest_influx_timestamp}")

    # Format the timestamp for SQLite and build the query
    formatted_timestamp = format_timestamp(oldest_influx_timestamp) if oldest_influx_timestamp else None
    sqlite_query = build_sqlite_query(formatted_timestamp)
    logging.info(f"Final SQLite query: {sqlite_query}")

    total_points = 0
    try:
        # Execute the SQLite query and process rows in batches
        logging.info("Fetching Data from SQLite.")
        cursor.execute(sqlite_query)
        rows_fetched = 0
        logging.info("Started Processing Data from SQLite.")
        while True:
            rows = cursor.fetchmany(BATCH_SIZE)
            if not rows:
                break
            batch_insert_to_influx(client, rows)
            rows_fetched += len(rows)
            total_points += len(rows)
            # logging.info(f"Processed {rows_fetched} rows so far.")
    except sqlite3.Error as e:
        logging.error(f"SQLite query error: {e}")
    finally:
        # Close connections (client is None in dry run mode)
        cursor.close()
        conn.close()
        if client is not None:
            client.close()
        logging.info("Closed connections to SQLite and InfluxDB")

    if DRY_RUN:
        logging.info(f"Dry run complete — {total_points} rows would have been processed (skipped rows excluded from count).")
    else:
        logging.info("Data export complete.")

if __name__ == "__main__":
    main()
