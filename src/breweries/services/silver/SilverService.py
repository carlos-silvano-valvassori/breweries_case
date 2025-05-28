from breweries.common.service import ABIInbevService
from breweries.common.spark import SparkSessionFactory
from breweries.common.medallion import Bronze, Silver
from pyspark.sql import functions as F
from breweries.services.spark_params import ADDITIONAL_PARAMETERS


class SilverService(ABIInbevService):
    NAME = "ABIInbevSilverService"

    def __init__(self):
        super().__init__(
            spark_session=SparkSessionFactory.get_session(additional_parameters=ADDITIONAL_PARAMETERS)
        )

    @property
    def service_layer(self) -> str:
        return "Silver"

    def _run(self, **kwargs):
        self.service_start_time()

        df_bronze = (Bronze()).read_parquet(self.spark)

        df_silver = (
            df_bronze
            .withColumn("address_1", F.when(F.col("address_1").isNull(), F.lit("N/A")).otherwise(F.col("address_1")))
            .withColumn("address_2", F.when(F.col("address_2").isNull(), F.lit("N/A")).otherwise(F.col("address_2")))
            .withColumn("address_3", F.when(F.col("address_3").isNull(), F.lit("N/A")).otherwise(F.col("address_3")))
        )
        Silver().write_parquet(df_silver, "append", ["country"])

        self.service_end_time()
