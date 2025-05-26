import os
from typing import Dict, Optional
import pyspark
from pyspark.conf import SparkConf


class SparkSessionFactory(object):
    """
    Factory class for SparkSession.

    This class has a factory method for SparkSessions, ``get_session()``. That method ensures that one single
    SparkSession exists in the lifecycle of an application.
    """

    _spark_session: Optional[pyspark.sql.SparkSession] = None

    @classmethod
    def get_session(
        cls, additional_parameters: Dict = None
    ) -> pyspark.sql.SparkSession:
        """
        Creates the SparkSession if needed and then returns it.

        @:param additional_parameters: Any new set of parameters when is required.
        Example:
        {"spark.broadcast.blockSize": "8g", "spark.executor.instances": "32", "spark.sql.broadcastTimeout: "3600"}

        Returns:
             A SparkSession created properly according to the configurations, able to use our Scala UDFs
        """
        if cls._spark_session is None:
            cls._spark_session = cls._create_session(additional_parameters)

        return cls._spark_session

    @classmethod
    def _default_session_config(cls):
        conf = SparkConf()
        conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        conf.set("spark.sql.adaptive.enabled", "true")
        conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        conf.set(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        conf.set("spark.sql.caseSensitive", "true")

        if "3.3" in pyspark.__version__:
            # Performance
            conf.set("spark.driver.memory", "20g")

            # Allowing VACUUM with retention_hours lower than 168 hours
            conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
            conf.set("spark.databricks.delta.vacuum.parallelDelete.enabled", "true")

            # Adapt dates to be able to be read and write on spark 3.0 from older versions of it
            conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "LEGACY")
            conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
            conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
            conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")

        return conf

    @classmethod
    def _build_session_config(cls, additional_parameters: Dict):
        conf = SparkSessionFactory._default_session_config()
        for key in additional_parameters:
            conf.set(key, additional_parameters[key])
        return conf

    @classmethod
    def _create_session(
        cls, additional_parameters: Dict = None
    ) -> pyspark.sql.SparkSession:
        print("Gets Spark Session")
        print(f"Additional Parameters: \n{additional_parameters}")
        print(f"pyspark is in {pyspark.__file__}")

        if additional_parameters is None:
            session_builder = (
                pyspark.sql.SparkSession.builder.config(
                    conf=SparkSessionFactory._default_session_config()
                )
                # Enable Hive Metastore
                .enableHiveSupport()
            )
        else:
            session_builder = (
                pyspark.sql.SparkSession.builder.config(
                    conf=SparkSessionFactory._build_session_config(
                        additional_parameters
                    )
                )
                # Enable Hive Metastore
                .enableHiveSupport()
            )

        session = (
            session_builder.appName("BreweriesCase")
            .config("spark.driver.maxResultSize", "0")
            .getOrCreate()
        )

        session.sparkContext.setLogLevel("WARN")

        return session


class LocalTestHarnessSparkSessionFactory(SparkSessionFactory):
    """
    Factory class for SparkSession to run in the local test harness.

    This class has a factory method for SparkSessions, ``get_session()``. That method ensures that one single
    SparkSession exists in the lifecycle of an application.
    """

    _spark_session: Optional[pyspark.sql.SparkSession] = None

    @classmethod
    def get_session(
        cls, additional_parameters: Dict = None
    ) -> pyspark.sql.SparkSession:
        """
        Creates the SparkSession if needed and then returns it.

        @:param additional_parameters: Any new set of parameters when is required.
        Example:
        {"spark.broadcast.blockSize": "8g", "spark.executor.instances": "32", "spark.sql.broadcastTimeout: "3600"}

        Returns:
             A SparkSession created properly according to the configurations, able to use our Scala UDFs
        """
        if cls._spark_session is None:
            cls._spark_session = cls._create_session(additional_parameters)

        return cls._spark_session

    @classmethod
    def _default_session_config(cls):
        conf = SparkConf()
        conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        conf.set(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        conf.set("fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY"))
        conf.set("spark.hadoop.fs.s3a.endpoint", os.environ.get("AWS_ENDPOINT_URL"))
        conf.set("fs.s3a.connection.ssl.enabled", "false")
        conf.set("fs.s3a.path.style.access", "true")
        conf.set("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        conf.set("spark.hadoop.fs.s3n.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        conf.set("spark.sql.caseSensitive", "true")
        return conf

    @classmethod
    def _build_session_config(cls, additional_parameters: Dict):
        conf = LocalTestHarnessSparkSessionFactory._default_session_config()
        if additional_parameters:
            for key in additional_parameters:
                conf.set(key, additional_parameters[key])
        return conf

    @classmethod
    def _create_session(
        cls, additional_parameters: Dict = None
    ) -> pyspark.sql.SparkSession:
        print("Gets Spark Session")
        print(f"Additional Parameters: \n{additional_parameters}")
        print(f"pyspark is in {pyspark.__file__}")

        session_builder = pyspark.sql.SparkSession.builder.config(
            conf=LocalTestHarnessSparkSessionFactory._build_session_config(
                additional_parameters
            )
        ).enableHiveSupport()

        is_prod = False

        app_name = "BreweriesCase"

        session = (
            session_builder.appName(app_name)
            .config("spark.driver.maxResultSize", "0")
            .getOrCreate()
        )
        session.sparkContext.setLogLevel("FATAL" if is_prod else "WARN")

        return session
