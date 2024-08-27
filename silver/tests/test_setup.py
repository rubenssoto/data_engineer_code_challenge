from pyspark.sql import SparkSession
from delta import *
import unittest
import logging


class TestBronze(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        builder = (
            SparkSession.builder.master("local[*]")
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )

        cls.spark = configure_spark_with_delta_pip(builder).getOrCreate()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()
