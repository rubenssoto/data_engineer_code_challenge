from chispa.dataframe_comparer import *
from datetime import date
from freezegun import freeze_time
import decimal
from datetime import datetime
from test_setup import TestBronze
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
    TimestampType,
    DateType,
)
import sys
from transformations.transformers import (
    DataTypeConverterTransformer,
    AddMetadataTransformer,
)


class TestDataTypeConverterTransformerTest(TestBronze):
    def test_data_type_converter_transformer(self):

        sample_schema = StructType(
            [
                StructField("age", StringType(), True),
                StructField("created_date", StringType(), True),
                StructField("updated_date", StringType(), True),
                StructField("salary", StringType(), True),
                StructField("last_login", StringType(), True),
            ]
        )

        sample_data = [
            ("25", "2024-08-20", "2024-08-21", "50000.00", "2024-08-24 12:00:00"),
            ("30", "2024-07-15", "2024-07-16", "60000.50", "2024-08-23 09:30:00"),
            ("22", "2024-06-10", "2024-06-11", "45000.75", "2024-08-22 18:45:00"),
            ("40", "2024-05-05", "2024-05-06", "70000.10", "2024-08-21 15:15:00"),
        ]

        sample_df = self.spark.createDataFrame(sample_data, sample_schema)

        transformer_config = {
            "age": "int",
            "created_date": "date",
            "updated_date": "date",
            "last_login": "timestamp",
            "salary": "decimal(10,2)",
        }

        result_df = DataTypeConverterTransformer(transformer_config).transform(
            df=sample_df
        )

        expected_schema = StructType(
            [
                StructField("age", IntegerType(), True),
                StructField("created_date", DateType(), True),
                StructField("updated_date", DateType(), True),
                StructField("salary", DecimalType(10, 2), True),
                StructField("last_login", TimestampType(), True),
            ]
        )

        expected_data = [
            (
                25,
                date(2024, 8, 20),
                date(2024, 8, 21),
                decimal.Decimal("50000.00"),
                datetime(2024, 8, 24, 12, 0, 0),
            ),
            (
                30,
                date(2024, 7, 15),
                date(2024, 7, 16),
                decimal.Decimal("60000.50"),
                datetime(2024, 8, 23, 9, 30, 0),
            ),
            (
                22,
                date(2024, 6, 10),
                date(2024, 6, 11),
                decimal.Decimal("45000.75"),
                datetime(2024, 8, 22, 18, 45, 0),
            ),
            (
                40,
                date(2024, 5, 5),
                date(2024, 5, 6),
                decimal.Decimal("70000.10"),
                datetime(2024, 8, 21, 15, 15, 0),
            ),
        ]

        expected_df = self.spark.createDataFrame(expected_data, expected_schema)

        assert_df_equality(result_df, expected_df)


class TestAddMetadataTransformer(TestBronze):
    def test_add_metadata_transformer(self):

        add_metadata = AddMetadataTransformer()

        sample_data = [
            (
                25,
                date(2024, 8, 20),
                date(2024, 8, 21),
                decimal.Decimal("50000.00"),
                datetime(2024, 8, 24, 12, 0, 0),
            ),
            (
                30,
                date(2024, 7, 15),
                date(2024, 7, 16),
                decimal.Decimal("60000.50"),
                datetime(2024, 8, 23, 9, 30, 0),
            ),
            (
                22,
                date(2024, 6, 10),
                date(2024, 6, 11),
                decimal.Decimal("45000.75"),
                datetime(2024, 8, 22, 18, 45, 0),
            ),
            (
                40,
                date(2024, 5, 5),
                date(2024, 5, 6),
                decimal.Decimal("70000.10"),
                datetime(2024, 8, 21, 15, 15, 0),
            ),
        ]

        sample_schema = StructType(
            [
                StructField("age", IntegerType(), True),
                StructField("created_date", DateType(), True),
                StructField("updated_date", DateType(), True),
                StructField("salary", DecimalType(10, 2), True),
                StructField("last_login", TimestampType(), True),
            ]
        )

        sample_df = self.spark.createDataFrame(sample_data, sample_schema)

        with freeze_time("2024-08-24 09:30:00"):
            df_result = add_metadata.transform(sample_df)

        expected_data = [
            (
                25,
                date(2024, 8, 20),
                date(2024, 8, 21),
                decimal.Decimal("50000.00"),
                datetime(2024, 8, 24, 12, 0, 0),
                datetime(2024, 8, 24, 9, 30, 0),
            ),
            (
                30,
                date(2024, 7, 15),
                date(2024, 7, 16),
                decimal.Decimal("60000.50"),
                datetime(2024, 8, 23, 9, 30, 0),
                datetime(2024, 8, 24, 9, 30, 0),
            ),
            (
                22,
                date(2024, 6, 10),
                date(2024, 6, 11),
                decimal.Decimal("45000.75"),
                datetime(2024, 8, 22, 18, 45, 0),
                datetime(2024, 8, 24, 9, 30, 0),
            ),
            (
                40,
                date(2024, 5, 5),
                date(2024, 5, 6),
                decimal.Decimal("70000.10"),
                datetime(2024, 8, 21, 15, 15, 0),
                datetime(2024, 8, 24, 9, 30, 0),
            ),
        ]

        expected_schema = StructType(
            [
                StructField("age", IntegerType(), True),
                StructField("created_date", DateType(), True),
                StructField("updated_date", DateType(), True),
                StructField("salary", DecimalType(10, 2), True),
                StructField("last_login", TimestampType(), True),
                StructField("load_at_utc", TimestampType(), True),
            ]
        )

        expected_df = self.spark.createDataFrame(expected_data, expected_schema)

        assert_df_equality(df_result, expected_df)
