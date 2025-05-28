import argparse
from abc import abstractmethod
from datetime import datetime
from pyspark.sql import SparkSession
from breweries.common import logger
from breweries.common.spark import SparkSessionFactory
import time


class ABIInbevService:
    NAME = "ABIInbev_Service"

    def __init__(self, spark_session: SparkSession = None):
        self._log = logger.get()
        self.start_at = None
        self.finish_at = None
        self.total = 0

        if spark_session is None:
            self.spark = SparkSessionFactory.get_session()
        else:
            self.spark = spark_session

    def service_start_time(self):
        self.start_at = time.time()

    def service_end_time(self):
        self.finish_at = time.time()
        self.total = self.finish_at - self.start_at
        self._log.info(f"Finished: in {self.total} seconds")

    @property
    @abstractmethod
    def service_layer(self) -> str:
        """The service layer. One of the following:
        * Bronze
        * Silver
        * Gold
        """
        pass

    def run(
        self,
        **kwargs,
    ):
        self._log.info(f"Starting ABIInbev_Service: {self.NAME}")
        self._log.info(f"Service Layer: {self.service_layer}")
        self._log.info(f"Input Arguments from service: {kwargs}")

        execution_start_time = datetime.now()

        # Run service
        self._run(**kwargs)

        execution_end_time = datetime.now()

        self._log.info("Finished.")
        self._log.info(f"Elapsed Time: {execution_end_time - execution_start_time}")

    @classmethod
    def add_arguments(cls, parser: argparse.ArgumentParser):
        """Add service specific arguments to the parser.

        Args:
        - parser: The parser to add arguments to.

        Returns:
        - parser: The parser with added arguments.
        """
        return parser

    @abstractmethod
    def _run(
        self,
        **kwargs,
    ):
        """Method to be overwritten with actual service implementation"""
        pass


