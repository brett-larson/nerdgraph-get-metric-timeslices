import csv
import os
from typing import List
from nerdgraph.utils.logger import Logger

# Create logger for the module
logger = Logger(__name__).get_logger()

class CsvWriter:
    def __init__(self, file_path: str):
        self.file_path = file_path

    def write_metric_names_to_csv(self, account_number: int, account_name: str, metric_names: List[str]):
        """
        Write unique metric names to a CSV file
        :param account_number: The account number of the metric
        :param account_name: The account name of the metric
        :param metric_names: A list of unique metric names
        """
        try:
            file_exists = os.path.isfile(self.file_path)
            with open(self.file_path, mode='a', newline='') as file:
                csv_writer = csv.writer(file)
                if not file_exists:
                    csv_writer.writerow(['account_number', 'account_name', 'metric_name'])
                for metric_name in metric_names:
                    csv_writer.writerow([account_number, account_name, metric_name])
        except Exception as e:
            logger.error(f"Error writing metric names to CSV: {e}")
            raise Exception(f"Error writing metric names to CSV: {e}")