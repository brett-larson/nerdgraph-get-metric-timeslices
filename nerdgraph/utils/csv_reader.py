import csv
from typing import List
from .logger import Logger

# Create logger for the module
logger = Logger(__name__).get_logger()

class CsvReader:
    def __init__(self, file_path: str):
        self.file_path = file_path

    def read_account_numbers(self) -> List[dict]:
        """
        Read account numbers from a CSV file
        :return: List of account numbers
        """
        accounts = []
        try:
            with open(self.file_path, mode='r') as file:
                csv_reader = csv.reader(file)
                headers = next(csv_reader)  # Skip the header
                for row in csv_reader:
                    accounts.append({
                        "account_number": int(row[0]),  # Read from the first column
                        "account_name": row[1]
                    })
        except Exception as e:
            logger.error(f"Error reading account numbers from CSV: {e}")
            raise Exception(f"Error reading account numbers from CSV: {e}")

        return accounts
