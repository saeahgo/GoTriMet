import re
import os
import csv
import datetime
import logging
import urllib.request
from google.cloud import pubsub_v1
from google.oauth2 import service_account
from bs4 import BeautifulSoup

class LoggerSetup:
    @staticmethod
    def init():
        date = datetime.datetime.now()
        log_dir = f'data/publisher_stop/logs/{date.year}/{date.month}'
        os.makedirs(log_dir, exist_ok=True)
        log_file = f'{log_dir}/publisher_stop_log_{date.year}-{date.month}-{date.day}.log'
        error_log_file = f'{log_dir}/publisher_stop_error_log_{date.year}-{date.month}-{date.day}.log'

        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[logging.FileHandler(log_file)]
        )
        error_logger = logging.getLogger('error')
        error_logger.addHandler(logging.FileHandler(error_log_file))
        error_logger.setLevel(logging.ERROR)

        return logging, error_logger

class VehicleLoader:
    def __init__(self, vehicle_file):
        self.vehicle_file = vehicle_file

    def load(self):
        with open(self.vehicle_file, 'r') as f:
            return [line.strip() for line in f.readlines()]

class StopEventFetcher:
    def __init__(self, base_url):
        self.base_url = base_url

    def fetch_html(self, vehicle_id):
        with urllib.request.urlopen(self.base_url + vehicle_id) as response:
            return response.read().decode('utf-8').strip()

    def parse_tables(self, html):
        soup = BeautifulSoup(html, 'html.parser')
        sections = []
        h2_elements = soup.find_all('h2')
        tables = soup.find_all('table')

        for h2, table in zip(h2_elements, tables):
            match = re.search(r'TRIP (\d+)', h2.text)
            if match:
                trip_id = match.group(1)
                sections.append((trip_id, table))

        return sections

class PubSubClient:
    def __init__(self, service_account_file, project_id, topic_id):
        creds = service_account.Credentials.from_service_account_file(service_account_file)
        self.publisher = pubsub_v1.PublisherClient(credentials=creds)
        self.topic_path = self.publisher.topic_path(project_id, topic_id)

    def publish(self, data):
        self.publisher.publish(self.topic_path, data.encode('utf-8'))

class StopEventPublisher:
    def __init__(self, config):
        self.date = datetime.datetime.now()
        self.vehicle_file = config['vehicle_file']
        self.base_url = config['base_url']
        self.pubsub_client = PubSubClient(
            config['service_account_file'],
            config['project_id'],
            config['topic_id']
        )
        self.fetcher = StopEventFetcher(self.base_url)
        self.logger, self.error_logger = LoggerSetup.init()
        self.error_dir = f'data/publisher_stop/errors/{self.date.year}/{self.date.month}'
        os.makedirs(self.error_dir, exist_ok=True)
        csv.field_size_limit(10 * 1024 * 1024)

    def log_error(self, error, vehicle_id):
        self.logger.error(f"Error fetching data for vehicle {vehicle_id}: {error}")
        with open(f'{self.error_dir}/{self.date.day}.txt', 'a') as f:
            f.write(f"{vehicle_id}\n")

    def run(self):
        message_count = 0
        vehicle_message_counts = {}
        vehicles = VehicleLoader(self.vehicle_file).load()

        for vehicle_id in vehicles:
            self.logger.info(f'Fetching stop events for Vehicle {vehicle_id}')
            vehicle_count = 0
            try:
                html = self.fetcher.fetch_html(vehicle_id)
                if not html:
                    self.logger.warning(f"No data for vehicle {vehicle_id}")
                    continue

                sections = self.fetcher.parse_tables(html)
                if not sections:
                    self.logger.warning(f"No table found for vehicle {vehicle_id}")
                    continue

                for trip_id, table in sections:
                    rows = table.find_all('tr')
                    if len(rows) <= 1:
                        continue

                    header = [th.get_text(strip=True) for th in rows[0].find_all('th')]
                    header.append('trip_id')  # Add trip_id column

                    row = rows[1]
                    cols = [td.get_text(strip=True) for td in row.find_all('td')]
                    if len(cols) != len(header) - 1: 
                        self.logger.warning(f"Column/header mismatch for vehicle {vehicle_id}")
                        continue

                    cols.append(trip_id)  # Add trip_id value
                    self.pubsub_client.publish('\t'.join(cols))
                    message_count += 1
                    vehicle_count += 1

            except Exception as e:
                self.log_error(e, vehicle_id)

            vehicle_message_counts[vehicle_id] = vehicle_count
            self.logger.info(f"Published {vehicle_count} messages for vehicle {vehicle_id}")

        self.logger.info("=== Per-Vehicle Message Count Summary ===")
        for v_id, count in vehicle_message_counts.items():
            self.logger.info(f"Vehicle {v_id}: {count} messages")
        self.logger.info(f"Total Stop Events messages published: {message_count}")
        print(f"Total Stop Events messages published: {message_count}")

if __name__ == "__main__":
    config = {
        "vehicle_file": "vehicles.txt",
        "base_url": "https://busdata.cs.pdx.edu/api/getStopEvents?vehicle_num=",
        "service_account_file": os.path.join(os.path.dirname(__file__), "pubsubkey.json"),
        "project_id": "trimet-data-pipeline",
        "topic_id": "stop-events-data"
    }

    publisher = StopEventPublisher(config)
    publisher.run()
