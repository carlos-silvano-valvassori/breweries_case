"""Module that provides helpers for working with Medallion configurations"""

from typing import List, Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


class Medallion:
    """Base class for Medallion configuration"""

    def __init__(self, url: str):
        self._path = url

    def __truediv__(self, other):
        """Return a new Medallion subpath equivalent to url / other"""
        new_url = f"{self._path}/{other}"
        return Medallion(new_url)

    @property
    def path(self) -> str:
        """Get the path of the medallion configuration"""
        return self._path

    def read_parquet(self, spark: SparkSession) -> DataFrame:
        """Read from parquet

        Arguments:
            spark (SparkSession): the current spark session

        Returns:
            DataFrame
        """
        return spark.read.format("parquet").load(self._path)

    def write_parquet(
        self,
        df: DataFrame,
        mode: str,
        partitionBy: Optional[List[str]] = None,
        replaceWhereCondition: Optional[str] = None,
        optimize_write: bool = True,
        overwrite_schema: bool = False,
        merge_schema: bool = False,
    ):
        """Write a DataFrame to parquet

        Arguments:
            df (DataFrame): the DataFrame to write
            mode (str): the mode (eg, "overwrite" or "append")
            partitionBy (Optional[List[str]]=None): columns to partition by
            replaceWhereCondition (Optional[str]=None): condition for replaceWhere option
            optimize_write (bool=True): whether to enable optimizeWrite
            overwrite_schema (bool=False): whether to enable overwriteSchema
            merge_schema (bool=False): whether to enable schema evolution

        Returns:
            number of output rows
        """
        df_writer = df.write.format("parquet")
        if partitionBy:
            df_writer = df_writer.partitionBy(*partitionBy)
        if replaceWhereCondition:
            df_writer = df_writer.option("replaceWhere", replaceWhereCondition)
        (
            df_writer.option("optimizeWrite", "true" if optimize_write else "false")
            .option("overwriteSchema", "true" if overwrite_schema else "false")
            .option("mergeSchema", "true" if merge_schema else "false")
            .mode(mode)
            .save(self._path)
        )


class Bronze(Medallion):
    """Bronze Medallion"""

    def __init__(self):
        super().__init__("tmp/bronze")


class Silver(Medallion):
    """Silver Medallion"""

    def __init__(self):
        super().__init__("tmp/silver")


class Gold(Medallion):
    """Gold Medallion"""

    def __init__(self):
        super().__init__("tmp/gold")

