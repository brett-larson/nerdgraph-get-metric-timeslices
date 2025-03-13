from nerdgraph.utils.nerdgraph_client import NerdGraphClient
from nerdgraph.utils.logger import Logger

# Create logger for the module
logger = Logger(__name__).get_logger()

class MetricTimeslices:
    GET_METRIC_TIMESLICES_QUERY = """
    query ($query: Nrql!, $account: Int!) {
      actor {
        nrql(query: $query, accounts: $account) {
          results
        }
      }
    }
    """

    def __init__(self, nerdgraph_client: NerdGraphClient):
        self.nerdgraph_client = nerdgraph_client

    def get_metric_timeslices(self, account_name: str, account_number: int, current_letter: str, timeout: int) -> dict:
        """
        Get metric timeslices using the app name, account ID, and current letter.
        :param account_name: Account name
        :param account_number: Account ID
        :param current_letter: Current letter
        :param timeout: GraphQL query timeout in seconds
        :return: dict
        """
        try:
            graphql_query, query_variables = self.build_query(account_number, current_letter, timeout)
            result = self.nerdgraph_client.execute_query(graphql_query, query_variables)

            if result.get("errors"):
                raise Exception(f"Failed to get metric timeslices for account {account_name} - {str(account_number)}: "
                                f"{result['errors']}")
            else:
                logger.info(f"Successfully got metric timeslices for app {account_name}.")
                return result["data"]["actor"]["nrql"]["results"][0]["uniques.metricTimesliceName"]
        except Exception as e:
            logger.error(f"Error getting metric timeslices: {e}")
            return {}

    @staticmethod
    def build_query(account_int: int, current_letter: str, timeout: int) -> tuple[str, dict[str, int | str]]:
        """
        Build the NRQL and GraphQL queries for getting metric timeslices.
        :param timeout:
        :param account_int: Account ID
        :param current_letter: Current letter
        :return: str
        """
        nrql_query = f"""FROM Metric SELECT uniques(metricTimesliceName) WHERE appName LIKE '%(live)%' AND metricTimesliceName LIKE '{current_letter}%' AND newrelic.timeslice.value IS NOT NULL SINCE 1 month ago"""

        query_variables = {
            "account": account_int,
            "query": nrql_query,
            "timeout": timeout
        }

        graphql_query = """
            query NrqlQuery2 ($query: Nrql!, $account: [Int!]!, $timeout: Seconds!) {
              actor {
                nrql(accounts: $account, query: $query, timeout: $timeout) {
                  results
                }
              }
            }
            """

        return graphql_query, query_variables

    @staticmethod
    def parse_metric_timeslices_response(response: dict) -> dict:
        """
        Parse the response from the get metric timeslices API.
        :param response: dict
        :return: dict
        """
        try:
            logger.info("Parsing metric timeslices response")
            if 'data' in response and 'actor' in response['data'] and 'nrql' in response['data']['actor']:
                return response['data']['actor']['nrql']['results']
            else:
                raise KeyError("Missing 'data', 'actor', or 'nrql' in response")
        except (KeyError, TypeError) as e:
            logger.error(f"Error parsing metric timeslices response: {e}")
            return {}
