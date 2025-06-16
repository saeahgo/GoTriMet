from load_to_postgres import load_breadcrumb_data

import datetime
import timeit
import os
import json
import logging
from google.cloud import pubsub_v1
from google.oauth2 import service_account
from collections import defaultdict, deque
import pandas as pd
import numpy as np
import threading
import signal
import time

# Global variables
df_lock = threading.Lock()
message_buffer = []
last_message_time = datetime.datetime.now()
COUNT = 0
idle_check_interval = 60  # Check idle time every 60 seconds
idle_threshold = 180  # 3 minutes threshold for idle period

# Logging setup
date = datetime.datetime.now()
start = timeit.default_timer()
log_dir = f'data/subscriber/logs/{date.year}/{date.month}'
os.makedirs(log_dir, exist_ok=True)
log_file_path = f'{log_dir}/subscriber_log_{date.year}-{date.month}-{date.day}.log'
error_log_file_path = f'{log_dir}/subscriber_error_log_{date.year}-{date.month}-{date.day}.log'

logger = logging.getLogger('main')
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
logger.setLevel(logging.DEBUG)  # Capture everything, filter with handlers

# Info handler
info_handler = logging.FileHandler(log_file_path)
info_handler.setLevel(logging.INFO)
info_handler.setFormatter(formatter)

# Error handler
error_handler = logging.FileHandler(error_log_file_path)
error_handler.setLevel(logging.ERROR)
error_handler.setFormatter(formatter)

logger.addHandler(info_handler)
logger.addHandler(error_handler)

# Service account file and subscription info
SERVICE_ACCOUNT_FILE = os.path.join(os.path.dirname(__file__), "pubsubkey.json")
project_id = "trimet-data-pipeline"
subscription_id = "bus-data-sub"

# Increment message count and log every 10,000 messages
def increment_message_count():
    global COUNT
    COUNT += 1
    if COUNT % 10000 == 0:
        logger.info(f"Received {COUNT} messages.")

# Process and save messages when idle for 3 minutes
def process_and_save():
    global message_buffer
    while True:
        time.sleep(idle_check_interval)
        current_time = datetime.datetime.now()
        time_since_last_message = (current_time - last_message_time).total_seconds()

        if time_since_last_message >= idle_threshold:
            with df_lock:
                if message_buffer:
                    logger.info("Detected 3 minutes of idle time. Running validation and saving data.")
                    process_buffer()
                else:
                    logger.info("Buffer is empty. Skipping validation and save.")

# Pub/Sub message processing
def process_message(json_obj):
    global message_buffer, last_message_time
    try:
        with df_lock:
            message_buffer.append(json_obj)
            increment_message_count()
        last_message_time = datetime.datetime.now()  # Update the last message time
    except Exception as e:
        logger.error(f"Error processing message: {e}")

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    try:
        json_obj = json.loads(message.data.decode("utf-8"))
        process_message(json_obj)
        message.ack()
    except Exception as e:
        logger.error(f"Error parsing message: {e}")
        message.nack()

# Existence Assertions
def check_required_fields(df: pd.DataFrame):
    required_fields = ["EVENT_NO_TRIP", "VEHICLE_ID", "ACT_TIME", "OPD_DATE"]
    for field in required_fields:
        if field not in df.columns or df[field].isnull().any():
            logger.warning(f"[EXISTENCE ASSERTION WARNING] Missing or null {field}")
    
# Limit Assertion: ACT_TIME should be between 0 and 97200 (until 3AM on the next day)
def check_act_time_limits(df: pd.DataFrame):
    if "ACT_TIME" in df.columns:
        invalid = df[(df["ACT_TIME"] < 0) | (df["ACT_TIME"] > 97200)]
        for idx in invalid.index:
            logger.warning(f"[LIMIT ASSERTION WARNING] Invalid ACT_TIME: {df.at[idx, 'ACT_TIME']}")

# Limit Assertion: METERS should be non-negative (>= 0)
def check_meters_non_negative(df: pd.DataFrame):
    if "METERS" in df.columns:
        invalid = df[df["METERS"] < 0]
        for idx in invalid.index:
            logger.warning(f"[LIMIT ASSERTION WARNING] Invalid METERS: {df.at[idx, 'METERS']}")

# Limit Assertion: GPS_HDOP should be positive (> 0)
def check_gps_hdop_positive(df: pd.DataFrame):
    if "GPS_HDOP" in df.columns:
        invalid = df[df["GPS_HDOP"] <= 0]
        for idx in invalid.index:
            logger.warning(f"[LIMIT ASSERTION WARNING] Invalid GPS_HDOP: {df.at[idx, 'GPS_HDOP']}")

# Intra-Record Assertion: GPS_LATITUDE and GPS_LONGITUDE cannot both be zero 
def check_zero_gps_coordinates(df: pd.DataFrame):
    if "GPS_LATITUDE" in df.columns and "GPS_LONGITUDE" in df.columns:
        zero_gps = df[(df["GPS_LATITUDE"] == 0) & (df["GPS_LONGITUDE"] == 0)]
        for idx in zero_gps.index:
            logger.warning(f"[INTRA RECORD ASSERTION WARNING] Both GPS_LATITUDE and GPS_LONGITUDE are zero in row {idx}")

# Inter-Record Assertion: For the same EVENT_NO_TRIP, METERS should increase or remain constant
def check_monotonic_meters(df: pd.DataFrame):
    if "EVENT_NO_TRIP" in df.columns and "METERS" in df.columns and "ACT_TIME" in df.columns:
        for trip_id, group in df.groupby("EVENT_NO_TRIP"):
            meters = group.sort_values("ACT_TIME")["METERS"]
            if not meters.is_monotonic_increasing:
                logger.warning(f"[INTER RECORD ASSERTION WARNING] METERS not monotonic for trip {trip_id}")

# Referential Integrity Assertion: VEHICLE_ID should be matched across all rows with the same EVENT_NO_TRIP
def check_vehicle_id_consistency(df: pd.DataFrame):
    if "EVENT_NO_TRIP" in df.columns and "VEHICLE_ID" in df.columns:
        mismatches = df.groupby("EVENT_NO_TRIP")["VEHICLE_ID"].nunique()
        for trip_id, count in mismatches.items():
            if count > 1:
                logger.warning(f"[REFERENTIAL INTEGRITY ASSERTION WARNING] Multiple VEHICLE_IDs for trip {trip_id}")

# Limit Assertion: LATITUDE and LONGITUDE should not be NULL
def remove_null_gps_coordinates(df: pd.DataFrame) -> pd.DataFrame:
    initial_len = len(df)
    df = df[df["GPS_LATITUDE"].notnull() & df["GPS_LONGITUDE"].notnull()]
    removed = initial_len - len(df)
    logger.info(f"Removed {removed} rows with null GPS_LATITUDE or GPS_LONGITUDE.")
    return df

# Limit Assertion: SPEED should be less than or equal to 80
def enforce_speed_limit(df: pd.DataFrame, max_speed: float = 80.0) -> pd.DataFrame:
    initial_len = len(df)
    df = df[df["SPEED"] <= max_speed]
    removed = initial_len - len(df)
    logger.info(f"[LIMIT ASSERTION] Removed {removed} rows with SPEED > {max_speed}.")
    return df

# Statistical/Distribution assertion: The ACT_TIME entries should be similar to uniform distribution across different times of the day
def assert_uniform_act_time_distribution(df: pd.DataFrame) -> None:
    if "ACT_TIME" in df.columns:
        bin_counts, _ = np.histogram(df["ACT_TIME"], bins=24, range=(0, 86400))
        mean = np.mean(bin_counts)
        std_dev = np.std(bin_counts)
        if std_dev > 0.2 * mean:
            logger.warning(
                f"[STATISTICAL ASSERTION WARNING] ACT_TIME distribution may be skewed. "
                f"Std Dev = {std_dev:.2f}, Mean = {mean:.2f}, Ratio = {std_dev / mean:.2f}"
            )
        else:
            logger.info("[STATISTICAL ASSERTION] ACT_TIME distribution appears uniform.")

# Summary assertion: The total number of EVENT_NO_TRIP entries must match the total number of unique trips in the dataset
def assert_trip_transitions(df: pd.DataFrame) -> None:
    if "EVENT_NO_TRIP" not in df.columns:
        logger.warning("[SUMMARY ASSERTION] EVENT_NO_TRIP column missing.")
        return

    event_trip_list = df["EVENT_NO_TRIP"].tolist()
    transition_count = 1 if event_trip_list else 0
    for i in range(1, len(event_trip_list)):
        if event_trip_list[i] != event_trip_list[i - 1]:
            transition_count += 1

    unique_trips = df["EVENT_NO_TRIP"].nunique()

    if transition_count != unique_trips:
        logger.warning(
            f"[SUMMARY ASSERTION WARNING] Unexpected trip transition count. "
            f"Transitions = {transition_count}, Unique Trips = {unique_trips}"
        )
    else:
        logger.info("[SUMMARY ASSERTION] Trip transition count matches unique trips.")

# Pre-Validation function
def run_all_validations(df: pd.DataFrame):
    validation_functions = [
        check_required_fields,
        check_act_time_limits,
        check_meters_non_negative,
        check_gps_hdop_positive,
        check_zero_gps_coordinates,
        check_monotonic_meters,
        check_vehicle_id_consistency
    ]

    for func in validation_functions:
        try:
            func(df)
        except Exception as e:
            logger.error(f"Validation function {func.__name__} failed: {e}")

# Validate and extract date from OPD_DATE for add_tstamp_column
def extract_date(opd_date_str):
    if isinstance(opd_date_str, str) and ':' in opd_date_str:
        try:
            date_part = opd_date_str.split(':')[0]
            return datetime.datetime.strptime(date_part, "%d%b%Y").date()
        except Exception:
            return None
    return None

# Transformation functions
def add_tstamp_column(df: pd.DataFrame) -> pd.DataFrame:
    try:
        logger.info("Extracting OPD_DATE...") 
        # Apply extraction and handle missing values
        df['OPD_DATE'] = df['OPD_DATE'].apply(extract_date)
        df["OPD_DATE"] = df["OPD_DATE"].fillna(method='ffill').fillna(method='bfill')

        # Ensure OPD_DATE is in datetime.date format
        df['OPD_DATE'] = pd.to_datetime(df['OPD_DATE'], errors='coerce').dt.date
        if df['OPD_DATE'].isnull().any():
            logger.error("Failed to resolve all OPD_DATE issues.")

        # Validate and convert ACT_TIME to timedelta
        df['TIME_DELTA'] = pd.to_timedelta(df['ACT_TIME'], unit='s', errors='coerce')
        if df['TIME_DELTA'].isnull().any():
            logger.warning("Some ACT_TIME values are invalid or missing.")

        # Combine date and time to create full timestamp
        df['TSTAMP'] = pd.to_datetime(df['OPD_DATE'].astype(str)) + df['TIME_DELTA']
        
    except Exception as e:
        logger.error(f"Failed to create tstamp: {e}")
        df["tstamp"] = pd.NaT
        return df
    return df

# Compute speed within each trip for add_speed_column
def compute_speed(group):
    meters = group["METERS"].values
    times = group["ACT_TIME"].values
    speeds = [0.0]
    for i in range(1, len(group)):
        delta_m = meters[i] - meters[i - 1]
        delta_t = times[i] - times[i - 1]
        speed = delta_m / delta_t if delta_t > 0 else 0.0
        speeds.append(speed)
    if len(speeds) > 1:
        speeds[0] = speeds[1]  # set first breadcrumb's speed = second breadcrumb's speed
    group["SPEED"] = speeds
    return group


def add_speed_column(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(by=["TSTAMP", "EVENT_NO_TRIP"])
    df["SPEED"] = 0.0  # default speed

    df = df.groupby("EVENT_NO_TRIP", group_keys=False).apply(compute_speed)
    return df

# Helper function for add_service_key_column
def get_service_key(date):
    # Get day of the week (0=Monday, 1=Tuesday, ..., 6=Sunday)
    day_of_week = date.weekday()  # Monday=0, Sunday=6
    if day_of_week == 5:  # Saturday
        return "Saturday"
    elif day_of_week == 6:  # Sunday
        return "Sunday"
    else:  # Weekday (Monday to Friday)
        return "Weekday"

def add_service_key_column(df: pd.DataFrame) -> pd.DataFrame:
    # Apply the service key based on TSTAMP
    df["SERVICE_KEY"] = df["TSTAMP"].apply(lambda x: get_service_key(x))

    return df

# Process the buffered messages
def process_buffer():
    global message_buffer
    try:
        df = pd.DataFrame(message_buffer)
        message_buffer = []  # Clear the buffer

        if df is None or df.empty:
            logger.warning("DataFrame is None or empty.")

        # Apply data validation and transformation
        run_all_validations(df)
        logger.info(f"Initial OPD_DATE values: {df['OPD_DATE'].head()}")
        logger.info(f"Initial ACT_TIME values: {df['ACT_TIME'].head()}")

        logger.info(f"Before add_tstamp_column: {df.shape}")
        df = add_tstamp_column(df) 
        logger.info(f"After add_tstamp_column: {df.shape}")
        df = add_speed_column(df)
        logger.info(f"After add_speed_column: {df.shape}")
        df = add_service_key_column(df)

        # Post-Validation checks
        # Remove rows with speed over 80
        df = enforce_speed_limit(df)

        #  Remove rows with null GPS coordinates
        df = remove_null_gps_coordinates(df)

        # Statistical/Distribution assertion: The ACT_TIME entries should be similar to uniform distribution across different times of the day
        assert_uniform_act_time_distribution(df)
        # Summary assertion: The total number of EVENT_NO_TRIP entries must match the total number of unique trips in the dataset
        assert_trip_transitions(df)

        # Save to Postgres
        load_breadcrumb_data(df, logger=logger)
        logger.info("Data successfully saved to Postgres.")

    except Exception as e:
        logger.error(f"Error processing buffer: {e}")

# Set up signal handling for graceful shutdown on SIGTERM
def shutdown_signal_handler(signum, frame):
    logger.info("SIGTERM received. Processing remaining messages before shutdown.")
    with df_lock:
        process_buffer()
    logger.info("Graceful shutdown complete.")
    exit(0)  # Exit cleanly

# Main function
def main():
    logger.info("Subscriber script started.")

    global last_message_time
    # Set up Pub/Sub client
    credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
    subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    logger.info(f"Listening for messages on {subscription_path}...")

    # Start idle monitoring in a separate thread
    threading.Thread(target=process_and_save, daemon=True).start()

    # Subscribe to messages
    flow_control = pubsub_v1.types.FlowControl(max_messages=1000, max_bytes=20 * 1024 * 1024)
    subscriber.subscribe(subscription_path, callback=callback, flow_control=flow_control)

    logger.info("Subscriber is listening for messages.")

    # Register the SIGTERM signal handler
    signal.signal(signal.SIGTERM, shutdown_signal_handler)

    # Keep the main thread running to listen for messages
    while True:
        time.sleep(60)

if __name__ == "__main__":
    main()
