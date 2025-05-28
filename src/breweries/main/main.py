from breweries.common.spark import SparkSessionFactory
from breweries.services.bronze.schema_definition import bronze_schema_breweries_by_country as schema
from breweries.common.brewery_api import *
import requests

COUNTRY = "United%20States"
url_api_country = f"https://api.openbrewerydb.org/v1/breweries?by_country={COUNTRY}"

# For testing only
if __name__ == "__main__":

    spark = SparkSessionFactory.get_session()

    us: SingleCountry = get_url_for_country(ISO_3166_US[INDEX_ISO_CODE])

    response = requests.get(us.url)

    json_result = response.json()

    df = spark.createDataFrame(json_result, schema)

    df.show(5)








