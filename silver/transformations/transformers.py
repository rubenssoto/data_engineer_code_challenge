from abc import ABC, abstractmethod
from pyspark.sql.dataframe import DataFrame
from datetime import datetime, timezone
from pyspark.sql.functions import lit
import logging

logger = logging.getLogger()


class AbstractTransformer(ABC):
    """
    An abstract base class that defines the interface for a transformer in the data pipeline.
    Transformers are responsible for applying some transformation to a DataFrame.
    Subclasses must implement the `transform` method to perform the desired transformation.
    """

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        pass

class DropDuplicatesTransformer(AbstractTransformer):

    def __init__(self, columns: list):
        self.columns = columns

    def transform(self, df: DataFrame) -> DataFrame:

        logger.info(f"Dropping duplicates for the columns: {self.columns}")
        return df.dropDuplicates(self.columns)
        

class DropDuplicatesTransformerFactory:
    
    def create(self, transformer_config: dict) -> DropDuplicatesTransformer:

        return DropDuplicatesTransformer(
            columns=transformer_config["drop_duplicates"].get("columns")
        )


class DataTypeConverterTransformer(AbstractTransformer):
    """
    Transforms a DataFrame by converting the data types of specified columns.

    The `DataTypeConverterTransformer` is an `AbstractTransformer` implementation that takes a configuration
    dictionary as input, where the keys are column names and the values are the desired data types for those columns.
    It then applies the specified data type conversions to the input DataFrame and returns the transformed DataFrame.

    Args:
        converter_config (dict): A dictionary mapping column names to their desired data types.

    Returns:
        DataFrame: The input DataFrame with the specified data type conversions applied.
    """

    def __init__(self, converter_config: dict):
        self.converter_config = converter_config

    def transform(self, df: DataFrame) -> DataFrame:

        for column_name, data_type in self.converter_config.items():

            logger.info(f"Converting column {column_name} to {data_type}")
            df = df.withColumn(column_name, df[column_name].cast(data_type))

        return df


class DataTypeConverterTransformerFactory:
    """
    A factory class that creates instances of the `DataTypeConverterTransformer` class.

    The `DataTypeConverterTransformerFactory` is responsible for creating instances of the `DataTypeConverterTransformer`
    class, which is an implementation of the `AbstractTransformer` interface. It takes a configuration
    dictionary as input, where the keys are column names and the values are the desired data types for those columns,
    and returns a `DataTypeConverterTransformer` instance that can be used to transform a DataFrame.
    """

    def create(self, transformer_config: dict) -> DataTypeConverterTransformer:

        return DataTypeConverterTransformer(
            converter_config=transformer_config.get("data_type_converter")
        )


class AddMetadataTransformer(AbstractTransformer):
    """
    Transforms a DataFrame by adding a new column "load_at_utc" with the current UTC timestamp.

    The `AddMetadataTransformer` is an `AbstractTransformer` implementation that adds a new column
    "load_at_utc" to the input DataFrame, containing the current UTC timestamp
    in the format "YYYY-MM-DD HH:MM:SS".

    Args:
        df (DataFrame): The input DataFrame to be transformed.

    Returns:
        DataFrame: The input DataFrame with the new "load_at_utc" column added.
    """

    def transform(self, df: DataFrame) -> DataFrame:

        load_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        logger.info(
            f"Adding the load_at_utc column with the current UTC timestamp: {load_at_utc}"
        )

        return df.withColumn("load_at_utc", lit(load_at_utc).cast("timestamp"))


class AddMetadataTransformerFactory:
    """
    A factory class that creates instances of the `AddMetadataTransformer` class.

    The `AddMetadataTransformerFactory` is responsible for creating instances of the `AddMetadataTransformer`
    class, which is an implementation of the `AbstractTransformer` interface. It takes a configuration
    dictionary as input and returns an `AddMetadataTransformer` instance.
    """

    def create(self, transformer_config: dict) -> AddMetadataTransformer:

        return AddMetadataTransformer()


class TransformerFactory:

    def get_transformer(transformer_config: dict):

        transformer_dict = {
            "add_metadata": AddMetadataTransformerFactory,
            "data_type_converter": DataTypeConverterTransformerFactory,
            "drop_duplicates": DropDuplicatesTransformerFactory,
        }

        return transformer_dict.get(list(transformer_config.keys())[0])().create(
            transformer_config=transformer_config
        )
