import os
import string
from dotenv import load_dotenv

from nerdgraph.utils import CsvWriter
from nerdgraph.utils.logger import Logger
from nerdgraph.utils.csv_reader import CsvReader
from nerdgraph.utils.nerdgraph_client import NerdGraphClient
from nerdgraph.queries.get_metric_timeslices import MetricTimeslices

# Create logger for the module
logger = Logger(__name__).get_logger()

def main():

    logger.info("********** Application started. **********")

    # Load environment variables
    load_dotenv()

    csv_file_path = "data/input/accounts.csv"
    csv_write_file_path = "data/output/timeslices_1.csv"
    graphql_timeout = 30
    csv_reader = CsvReader(csv_file_path)
    csv_writer = CsvWriter(csv_write_file_path)
    nerdgraph_client = NerdGraphClient()
    metric_timeslices = MetricTimeslices(nerdgraph_client)

    # Get account numbers and names from the CSV file
    try:
        accounts = csv_reader.read_account_numbers()
        number_of_accounts = len(accounts)
        logger.info(f"Number of accounts: {number_of_accounts}")
        # print(f"Number of accounts: {number_of_accounts}")
    except Exception as e:
        logger.error(f"Error reading account numbers from CSV: {e}")
        exit(1) # Exit the application if there is an error reading the CSV file.

    for account in accounts:
        timeslices = []
        account_number = account["account_number"]
        account_name = account["account_name"]
        logger.info(f"Gathering unique timeslice data for account: {account_number}, Account Name: {account_name}")

        for letter in string.ascii_lowercase:
            logger.info(f"Gathering timeslice data for letter: {letter}")
            try:
                timeslices = metric_timeslices.get_metric_timeslices(account_name=account_name,
                                                                     account_number=account_number,
                                                                     current_letter=letter,
                                                                     timeout=graphql_timeout)
            except Exception as e:
                logger.error(f"Error getting metric timeslices: {e}")

            # Write the timeslices to a CSV file
            try:
                logger.info(f"Writing timeslice data for letter: {letter}")
                csv_writer.write_metric_names_to_csv(account_number, account_name, timeslices)
            except Exception as e:
                logger.error(f"Error writing timeslices to CSV: {e}")

    logger.info("********** Application finished. **********")

if __name__ == "__main__":
    main()