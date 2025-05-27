from load_to_postgres import load_stop_event_data

import os
import csv
import signal
import threading
import datetime
import logging
import pandas as pd
from google.cloud import pubsub_v1
from google.oauth2 import service_account

class LoggerSetup:
    @staticmethod
    def init():
        date = datetime.datetime.now()
        log_dir = f'data/subscriber_stop/logs/{date.year}/{date.month}'
        os.makedirs(log_dir, exist_ok=True)
        log_file = f'{log_dir}/subscriber_stop_log_{date.year}-{date.month}-{date.day}.log'
        error_log_file = f'{log_dir}/subscriber_stop_error_log_{date.year}-{date.month}-{date.day}.log'
        
        # Main logger
        logger = logging.getLogger("subscriber_stop")
        logger.setLevel(logging.INFO)
        logger.propagate = False # prevent show logs in the stdout

        # Prevent duplicated logs
        if not logger.handlers:
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
            logger.addHandler(file_handler)

        # Error logger
        error_logger = logging.getLogger("subscriber_stop_error")
        error_logger.setLevel(logging.ERROR)
        error_logger.propagate = False  # prevent show logs in the stdout
        if not error_logger.handlers:
            error_handler = logging.FileHandler(error_log_file)
            error_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
            error_logger.addHandler(error_handler)

        return logger, error_logger

class StopEventSubscriber:
    def __init__(self, config):
        self.logger, self.error_logger = LoggerSetup.init()
        self.date = datetime.datetime.now()
        self.shutdown_event = threading.Event()
        self.last_message_time = datetime.datetime.now()
        self.received_any_message = False
        self.rows = [] # raw incoming messages

        creds = service_account.Credentials.from_service_account_file(config["service_account_file"])
        self.subscriber = pubsub_v1.SubscriberClient(credentials=creds)
        self.subscription_path = self.subscriber.subscription_path(
            config["project_id"], config["subscription_id"]
        )

    def callback(self, message):
        try:
            decoded = message.data.decode('utf-8').strip().split('\t')
            self.rows.append(decoded)
            self.received_any_message = True
            self.last_message_time = datetime.datetime.now()  # Update last message time
            message.ack()
        except Exception as e:
            self.error_logger.error(f"Failed to process message: {e}")
            message.nack()

    def monitor_inactivity(self):
        while not self.shutdown_event.is_set():
            now = datetime.datetime.now()
            if self.received_any_message and (now - self.last_message_time).total_seconds() > 180:  # 3 minutes of inactivity
                self.logger.info("No messages received for 3 minutes. Saving data and loading to database.")
                self.save_and_load_data()
                self.received_any_message = False
            else:
                self.logger.info("Still waiting for messages...")
            self.shutdown_event.wait(timeout=60)

    def save_and_load_data(self):
        if not self.rows:
            self.logger.info("No rows collected. Skipping.")
            return

        try:
            df = pd.DataFrame(self.rows)
            if df.empty or df.shape[1] < 6:
                self.logger.warning("Insufficient data. Skipping.")
                self.rows.clear()
                return

            # Column selection and renaming
            df = df.iloc[:, [ -1, 0, 3, 5, 4 ]] 
            df.columns = ["trip_id", "vehicle_number", "route_number", "service_key", "direction"]

            load_stop_event_data(df)
            self.logger.info("Data successfully loaded into the database.")

        except Exception as e:
            self.error_logger.error(f"Failed to load data into the database: {e}")
        finally:
            self.rows.clear()


    def run(self):
        streaming_pull_future = self.subscriber.subscribe(self.subscription_path, callback=self.callback)
        print(f"Listening for messages from Google Pub/Sub...")
        self.logger.info(f"Listening for messages from Google Pub/Sub...")

        # Start the inactivity monitor thread
        monitor_thread = threading.Thread(target=self.monitor_inactivity, daemon=True)
        monitor_thread.start()

        def handle_signal(signum, frame):
            self.logger.info(f"Signal {signum} received. Initiating shutdown...")
            self.shutdown_event.set()
            streaming_pull_future.cancel()

        # Register signal handlers
        signal.signal(signal.SIGTERM, handle_signal)
        signal.signal(signal.SIGINT, handle_signal)

        try:
            while not self.shutdown_event.is_set():
                self.shutdown_event.wait(timeout=1)
        finally:
            self.logger.info("Final shutdown. Saving any remaining data.")
            self.save_and_load_data()
            self.logger.info(f"Total messages processed: {len(self.rows)}")
            print(f"Total messages processed: {len(self.rows)}")

if __name__ == "__main__":
    config = {
        "service_account_file": os.path.join(os.path.dirname(__file__), "pubsubkey.json"),
        "project_id": "trimet-data-pipeline",
        "subscription_id": "stop-events-data-sub"
    }

    subscriber = StopEventSubscriber(config)
    subscriber.run()
