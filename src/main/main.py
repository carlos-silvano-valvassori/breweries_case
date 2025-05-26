from src.common.spark import SparkSessionFactory
from src.common.structures import schema
from src.common.brewery_api import ISO_3166_US, SingleCountry, get_url_for_country
import requests

COUNTRY = "United%20States"
url_api_country = f"https://api.openbrewerydb.org/v1/breweries?by_country={COUNTRY}"


if __name__ == "__main__":

    spark = SparkSessionFactory.get_session()

    us: SingleCountry = get_url_for_country(ISO_3166_US)

    response = requests.get(us.url)

    json_result = response.json()

    df = spark.createDataFrame(json_result, schema)

    df.show(5)








