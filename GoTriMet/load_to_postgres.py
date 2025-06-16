# load_to_postgres.py

import io
import os
import sys
import logging
import tempfile
import psycopg2
import pandas as pd

from dotenv import load_dotenv
load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)

def require_env(var):
    value = os.getenv(var)
    if value is None:
        raise EnvironmentError(f"Missing required environment variable: {var}")
    return value


def create_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS trip (
                trip_id     INTEGER PRIMARY KEY,
                route_id    INTEGER,
                vehicle_id  INTEGER,
                service_key TEXT,
                direction   TEXT
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS breadcrumb (
                tstamp      TIMESTAMP,
                latitude    DOUBLE PRECISION,
                longitude   DOUBLE PRECISION,
                speed       DOUBLE PRECISION,
                trip_id     INTEGER REFERENCES trip(trip_id),
                PRIMARY KEY (tstamp, trip_id)
            );
        """)
    conn.commit()


def load_breadcrumb_data(df: pd.DataFrame, logger=None):
    if logger is None:
        logger = logging.getLogger(__name__)  # fallback
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(handler)

    logger.info("Loading data to PostgreSQL...")
    df.columns = df.columns.str.lower()
    df["tstamp"] = pd.to_datetime(df["tstamp"])

    # Prepare trip and breadcrumb DataFrames
    breadcrumb_df = df.rename(columns={
        'gps_latitude': 'latitude',
        'gps_longitude': 'longitude',
        'event_no_trip': 'trip_id'
    })[['tstamp', 'latitude', 'longitude', 'speed', 'trip_id']]

    trip_df = df.rename(columns={
        'event_no_trip': 'trip_id'
    })[['trip_id', 'vehicle_id', 'service_key']]

    trip_df['route_id'] = -1
    trip_df['direction'] = '0'

    trip_df = trip_df.drop_duplicates(subset="trip_id")
    breadcrumb_df.drop_duplicates(subset=["tstamp", "trip_id"], inplace=True)
    breadcrumb_df = breadcrumb_df[breadcrumb_df['trip_id'].isin(trip_df['trip_id'])]

    if breadcrumb_df.empty:
        logger.error("No breadcrumb data to load.")
        return 0

    # Convert to in-memory CSV buffers
    trip_buf = io.StringIO()
    breadcrumb_buf = io.StringIO()

    trip_df.to_csv(trip_buf, index=False, header=False,
                   columns=["trip_id", "route_id", "vehicle_id", "service_key", "direction"])
    breadcrumb_df.to_csv(breadcrumb_buf, index=False, header=False,
                         columns=["tstamp", "latitude", "longitude", "speed", "trip_id"])

    # Reset buffer position
    trip_buf.seek(0)
    breadcrumb_buf.seek(0)

    # Connect to DB and load data
    conn = psycopg2.connect(
        dbname=require_env("DB_NAME"),
        user=require_env("DB_USER"),
        password=require_env("DB_PASSWORD"),
        host=require_env("DB_HOST")
    )
    
    cur = conn.cursor()
    create_tables(conn)

    # Disable constraints temporarily
    cur.execute("SET session_replication_role = 'replica';")

    # Create temp tables
    cur.execute("""
        CREATE TEMP TABLE tmp_trip (
            trip_id INTEGER,
            route_id INTEGER,
            vehicle_id INTEGER,
            service_key TEXT,
            direction TEXT
        );
    """)
    cur.execute("""
        CREATE TEMP TABLE tmp_breadcrumb (
            tstamp TIMESTAMP,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            speed DOUBLE PRECISION,
            trip_id INTEGER
        );
    """)

    # Load in-memory CSV data into temp tables
    cur.copy_expert("COPY tmp_trip (trip_id, route_id, vehicle_id, service_key, direction) FROM STDIN WITH CSV", trip_buf)
    cur.copy_expert("COPY tmp_breadcrumb (tstamp, latitude, longitude, speed, trip_id) FROM STDIN WITH CSV", breadcrumb_buf)

    # Count new rows
    cur.execute("SELECT COUNT(*) FROM tmp_trip")
    new_trip_rows = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM tmp_breadcrumb")
    new_breadcrumb_rows = cur.fetchone()[0]

    # Insert with deduplication
    cur.execute("""
        INSERT INTO trip (trip_id, route_id, vehicle_id, service_key, direction)
        SELECT * FROM tmp_trip
        ON CONFLICT (trip_id) DO NOTHING;
    """)
    cur.execute("""
        INSERT INTO breadcrumb (tstamp, latitude, longitude, speed, trip_id)
        SELECT * FROM tmp_breadcrumb
        ON CONFLICT (tstamp, trip_id) DO NOTHING;
    """)

    conn.commit()

    # Create indexes
    cur.execute("CREATE INDEX IF NOT EXISTS idx_trip_vehicle ON trip(vehicle_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_breadcrumb_trip ON breadcrumb(trip_id);")
    conn.commit()

    cur.close()
    conn.close()

    logger.info(f"{new_trip_rows} rows were added to the Trip table.")
    logger.info(f"{new_breadcrumb_rows} rows were added to the Breadcrumb table.")

    logger.info("Data load complete.")
    print("Data load complete.")
    return 1

def create_stop_event_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS stop_event (
                trip_id		INTEGER PRIMARY KEY,
                vehicle_number	INTEGER,
                route_number	INTEGER,
                service_key	TEXT,
                direction	INTEGER
            );
        """)
    conn.commit()


def load_stop_event_data(df: pd.DataFrame):
    required_columns = ["trip_id", "vehicle_number", "route_number", "service_key", "direction"]
    if not all(col in df.columns for col in required_columns):
        raise ValueError(f"DataFrame must contain columns: {required_columns}")

    df.columns = df.columns.str.lower()

    # Convert DataFrame to in-memory CSV
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    with psycopg2.connect(
        dbname=require_env("DB_NAME"),
        user=require_env("DB_USER"),
        password=require_env("DB_PASSWORD"),
        host=require_env("DB_HOST")
    ) as conn:
        create_stop_event_table(conn)

        with conn.cursor() as cur:
            # Create temporary table
            cur.execute("""
                CREATE TEMP TABLE tmp_stop_event (
                    trip_id INTEGER,
                    vehicle_number INTEGER,
                    route_number INTEGER,
                    service_key TEXT,
                    direction INTEGER
                );
            """)

            # Load buffer into temp table
            cur.copy_expert("""
                COPY tmp_stop_event (trip_id, vehicle_number, route_number, service_key, direction)
                FROM STDIN WITH CSV
            """, buffer)

            # Count new data rows
            cur.execute("SELECT COUNT(*) FROM tmp_stop_event")
            new_stop_rows = cur.fetchone()[0]


            # Insert into final table with deduplication
            cur.execute("""
                INSERT INTO stop_event (trip_id, vehicle_number, route_number, service_key, direction)
                SELECT * FROM tmp_stop_event
                ON CONFLICT (trip_id, vehicle_number) DO NOTHING;
            """)

            # INTEGRATION: Update trip table based on new stop_event rows
            cur.execute("""
                UPDATE trip
                SET 
                  route_id = se.route_number,
                  service_key = se.service_key,
                  direction = se.direction
                FROM stop_event se
                WHERE trip.trip_id = se.trip_id;
            """)

        conn.commit()
        logger.info(f"{new_stop_rows} rows were added to the Stop_Event table.")
        logger.info("Stop event data load complete and trip table updated.")
