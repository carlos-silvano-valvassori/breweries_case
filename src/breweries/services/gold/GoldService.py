from breweries.common.service import ABIInbevService
from breweries.common.spark import SparkSessionFactory
from breweries.common.medallion import Silver, Gold
from breweries.services.spark_params import ADDITIONAL_PARAMETERS


class GoldService(ABIInbevService):
    NAME = "ABIInbevGoldService"

    def __init__(self):
        super().__init__(
            spark_session=SparkSessionFactory.get_session(additional_parameters=ADDITIONAL_PARAMETERS)
        )

    @property
    def service_layer(self) -> str:
        return "Gold"

    def _run(self, **kwargs):
        self.service_start_time()

        df_silver = (Silver()).read_parquet(self.spark)

        # Create an aggregated view with the quantity of breweries per type and location.
        df_agg_location = (
            df_silver
            .groupBy("country")
            .count()
        )
        df_agg_type = (
            df_silver
            .groupBy("brewery_type")
            .count()
        )

        (Gold() / "agg_by_country").write_parquet(df_agg_location, "append", ["country"])

        (Gold() / "agg_by_brewery_type").write_parquet(df_agg_type, "append", ["brewery_type"])

        df_gold_country = (Gold() / "agg_by_country").read_parquet(self.spark)

        df_gold_type = (Gold() / "agg_by_brewery_type").read_parquet(self.spark)

        df_gold_country.createOrReplaceTempView("breweries_per_country")

        df_gold_type.createOrReplaceTempView("breweries_per_type")

        self.service_end_time()
