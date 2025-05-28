from breweries.common.service import ABIInbevService
from breweries.common.spark import SparkSessionFactory
from breweries.common.brewery_api import get_list_of_countries, SingleCountry, get_url_for_country
from breweries.services.spark_params import ADDITIONAL_PARAMETERS
from breweries.common.medallion import Bronze
from breweries.services.bronze.schema_definition import bronze_schema_breweries_by_country as schema
import requests


class BronzeService(ABIInbevService):
    NAME = "ABIInbevBronzeService"

    def __init__(self):
        super().__init__(
            spark_session=SparkSessionFactory.get_session(additional_parameters=ADDITIONAL_PARAMETERS)
        )

    @property
    def service_layer(self) -> str:
        return "Bronze"

    def _run(self, **kwargs):
        self.service_start_time()

        country_list = get_list_of_countries()

        for country in country_list:
            country_variant: SingleCountry = get_url_for_country(country)

            self._log.info(f"Current country check: [{country}][url: {country_variant.url}]")

            response = requests.get(country_variant.url)

            json_result = response.json()

            df = self.spark.createDataFrame(json_result, schema)

            Bronze().write_parquet(df, "append", ["country"])

        self.service_end_time()
