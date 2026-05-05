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
IMPORT_STATISTICS_DATA = os.getenv("IMPORT_STATISTICS_DATA", "false").lower() == "true"
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

def get_entity_ids(cursor):
    """Fetch all distinct entity_ids from SQLite."""
    cursor.execute("SELECT entity_id FROM states_meta")
    return [row[0] for row in cursor.fetchall()]

def get_entity_uom(cursor, entity_id):
    """Fetch the unit_of_measurement for an entity_id."""
    cursor.execute("""
        SELECT json_extract(sa.shared_attrs, '$.unit_of_measurement')
        FROM states s
        JOIN states_meta sm ON sm.metadata_id = s.metadata_id
        JOIN state_attributes sa ON sa.attributes_id = s.attributes_id
        WHERE sm.entity_id = ? AND json_extract(sa.shared_attrs, '$.unit_of_measurement') IS NOT NULL
        LIMIT 1
    """, (entity_id,))
    row = cursor.fetchone()
    return row[0] if row else None

def get_entity_rows(cursor, entity_id, batch_size, oldest_ts):
    """Yield batches of rows for a specific entity_id that are older than oldest_ts."""
    query = """
    SELECT s.state, sm.entity_id, s.last_updated_ts, sa.shared_attrs
    FROM states s
    LEFT JOIN state_attributes sa ON sa.attributes_id = s.attributes_id
    JOIN states_meta sm ON sm.metadata_id = s.metadata_id
    WHERE sm.entity_id = ? AND s.last_updated_ts < ?
    ORDER BY s.last_updated_ts DESC
    """
    cursor.execute(query, (entity_id, oldest_ts.timestamp()))
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        yield rows

def get_entity_statistics_rows(cursor, entity_id, batch_size, oldest_ts):
    """Yield batches of statistics rows for a specific entity_id that are older than oldest_ts."""
    query = """
    SELECT s.start_ts, s.mean, s.min, s.max, s.state, s.sum
    FROM statistics s
    JOIN statistics_meta sm ON sm.id = s.metadata_id
    WHERE sm.statistic_id = ? AND s.start_ts < ?
    ORDER BY s.start_ts DESC
    """
    cursor.execute(query, (entity_id, oldest_ts.timestamp()))
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        yield rows

def parse_attributes(shared_attrs):
    try:
        # Parse the shared attributes JSON
        return json.loads(shared_attrs)
    except (TypeError, json.JSONDecodeError) as e:
        logging.warning(f"Failed to parse attributes: {e}")
        return {}

_entity_oldest_ts_cache = {}

def get_entity_oldest_timestamp(client, measurement, domain, entity_id_short):
    if client is None:
        return datetime.now(timezone.utc) # For dry-run without client, assume very new to allow insert
    cache_key = f"{measurement}:{domain}:{entity_id_short}"
    if cache_key in _entity_oldest_ts_cache:
        return _entity_oldest_ts_cache[cache_key]
    try:
        query = f'SELECT * FROM "{measurement}" WHERE "domain" = \'{domain}\' AND "entity_id" = \'{entity_id_short}\' ORDER BY time ASC LIMIT 1'
        table = client.query(query=query, language="influxql")
        if table is not None and table.num_rows > 0:
            time_col = table.column("time")
            if time_col and len(time_col) > 0:
                ts = time_col[0].as_py()
                _entity_oldest_ts_cache[cache_key] = ts
                return ts
        _entity_oldest_ts_cache[cache_key] = None
        return None
    except Exception as e:
        logging.warning(f"Error checking entity oldest timestamp for {cache_key}: {e}")
        _entity_oldest_ts_cache[cache_key] = None
        return None

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

        try:
            # Convert timestamp from Unix epoch to UTC-aware datetime object
            last_updated_dt = datetime.fromtimestamp(float(last_updated_ts), tz=timezone.utc)
            
            # Create an InfluxDB point with tags and fields
            point = Point(unit_of_measurement).tag("source", "states").tag("domain", domain)
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
                    logging.debug(f"Writing point: {point}")
                    client.write(record=point)
                except Exception as e:
                    logging.error(f"Error writing point to InfluxDB: {e}. Point: {point}")
                    raise e
        else:
            try:
                logging.debug(f"Writing {len(points)} points to InfluxDB")
                client.write(record=points)
                logging.info(f"Successfully wrote {len(points)} points to InfluxDB")
            except Exception as e:
                logging.error(f"Error writing points to InfluxDB: {e}")
                raise e
    else:
        logging.info("No points to write in this batch.")

def batch_insert_statistics_to_influx(client, rows, entity_id, uom):
    points = []
    domain, _, entity_id_short = entity_id.partition('.')
    for row in rows:
        start_ts, mean_val, min_val, max_val, state_val, sum_val = row
        
        # For the value column, take the mean from the statistics table, if that is null use the state column.
        value = mean_val if mean_val is not None else state_val
        if value is None:
            continue
            
        try:
            start_dt = datetime.fromtimestamp(float(start_ts), tz=timezone.utc)
            point = Point(uom).tag("source", "statistics").tag("domain", domain).tag("entity_id", entity_id_short).time(start_dt)
            
            point.field("value", float(value))
            if mean_val is not None:
                point.field("mean", float(mean_val))
            if min_val is not None:
                point.field("min", float(min_val))
            if max_val is not None:
                point.field("max", float(max_val))
            if state_val is not None:
                point.field("state", float(state_val))
            if sum_val is not None:
                point.field("sum", float(sum_val))
                
            points.append(point)
        except Exception as e:
            logging.warning(f"Error preparing statistics point for {entity_id}: {e}")
            
    if points:
        if DRY_RUN:
            for point in points:
                print(point.to_line_protocol())
        elif DEBUG_MODE:
            for point in points:
                try:
                    logging.debug(f"Writing stat point: {point}")
                    client.write(record=point)
                except Exception as e:
                    logging.error(f"Error writing stat point to InfluxDB: {e}. Point: {point}")
                    raise e
        else:
            try:
                logging.debug(f"Writing {len(points)} stat points to InfluxDB")
                client.write(record=points)
            except Exception as e:
                logging.error(f"Error writing stat points to InfluxDB: {e}")
                raise e

def main():
    # Main execution flow
    conn, cursor = connect_to_sqlite(sqlite_db)
    client = connect_to_influxdb(influx_url, influx_token, influx_database)

    total_states_points = 0
    total_statistics_points = 0
    try:
        logging.info("Fetching entity IDs from SQLite.")
        entity_ids = get_entity_ids(cursor)
        logging.info(f"Found {len(entity_ids)} entities in SQLite.")

        entities_to_process = []
        for entity_id in entity_ids:
            uom = get_entity_uom(cursor, entity_id)
            if not uom:
                continue
                
            domain, _, entity_id_short = entity_id.partition('.')
            oldest_ts = get_entity_oldest_timestamp(client, uom, domain, entity_id_short)
            
            if oldest_ts is None:
                logging.debug(f"Discarding entity '{entity_id}' - no existing records in InfluxDB.")
                continue
                
            entities_to_process.append((entity_id, uom, oldest_ts))

        logging.info(f"Found {len(entities_to_process)} entities with existing InfluxDB records to process.")

        for entity_id, uom, oldest_ts in entities_to_process:
            logging.info(f"Processing entity states: {entity_id}")
            entity_states_points = 0
            for rows in get_entity_rows(cursor, entity_id, BATCH_SIZE, oldest_ts):
                batch_insert_to_influx(client, rows)
                entity_states_points += len(rows)
            total_states_points += entity_states_points
            
            if IMPORT_STATISTICS_DATA:
                logging.info(f"Processing entity statistics: {entity_id}")
                entity_statistics_points = 0
                for rows in get_entity_statistics_rows(cursor, entity_id, BATCH_SIZE, oldest_ts):
                    batch_insert_statistics_to_influx(client, rows, entity_id, uom)
                    entity_statistics_points += len(rows)
                total_statistics_points += entity_statistics_points
                logging.info(f"Finished '{entity_id}' — States: {entity_states_points}, Statistics: {entity_statistics_points}")
            else:
                logging.info(f"Finished '{entity_id}' — States: {entity_states_points}")
    except sqlite3.Error as e:
        logging.error(f"SQLite query error: {e}")
    finally:
        # Close connections (client is None in dry run mode)
        cursor.close()
        conn.close()
        if client is not None:
            client.close()
        logging.info("Closed connections to SQLite and InfluxDB")

    total_processed = total_states_points + total_statistics_points
    if DRY_RUN:
        logging.info(f"Dry run complete — {total_processed} rows would have been processed "
                     f"(States: {total_states_points}, Statistics: {total_statistics_points}).")
    else:
        logging.info(f"Data export complete — Processed {total_processed} rows "
                     f"(States: {total_states_points}, Statistics: {total_statistics_points}).")

if __name__ == "__main__":
    main()
