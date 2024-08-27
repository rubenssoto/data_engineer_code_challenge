from abc import ABC, abstractmethod
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession
from delta.tables import *
import logging

logger = logging.getLogger()


class AbstractFileWriter(ABC):
    """
    An abstract base class that defines the interface for file writers. Concrete
    implementations of this class should provide a way to write data to a file system.
    """

    def __init__(self, write_path: str, primary_key: list[str], table_name: str):
        self.write_path = write_path
        self.primary_key = primary_key
        self.table_name = table_name

    @abstractmethod
    def write(self, df: DataFrame) -> None:
        pass


class DeltaLakeMergeWriter(AbstractFileWriter):
    """
    A concrete implementation of the `AbstractFileWriter` class that writes data to a Delta Lake table using a merge operation.

    The `DeltaLakeMergeWriter` class is responsible for writing DataFrame data to a Delta Lake table located at the specified `write_path`.
    It uses a merge operation to update existing records and insert new records.

    The class takes the following parameters in its constructor:
    - `write_path`: The base path where the Delta Lake table is located.
    - `primary_key`: A list of column names that represent the primary key for the table.
    - `table_name`: The name of the Delta Lake table.

    The `write` method takes a DataFrame as input and writes it to the Delta Lake table. If the table already exists,
    it performs a merge operation to update existing records and insert new records. If the table does not exist,
    it creates a new Delta Lake table and writes the data to it.
    """

    def __init__(self, write_path: str, primary_key: list[str], table_name: str):
        self.write_path = write_path
        self.primary_key = primary_key
        self.table_name = table_name

    def write(self, df: DataFrame, spark: SparkSession) -> None:

        full_write_path = f"{self.write_path}/{self.table_name}"

        if DeltaTable.isDeltaTable(spark, full_write_path):

            merge_condition = None
            for key in self.primary_key:

                if merge_condition is None:
                    merge_condition = f"target.{key} = source.{key}"
                else:
                    merge_condition += f" and target.{key} = source.{key}"

            logger.info(f"Merge condition: {merge_condition}")

            delta_table = DeltaTable.forPath(spark, full_write_path)

            logger.info(f"Merging data into {full_write_path}")
            delta_table.alias("target").merge(
                df.alias("source"), merge_condition
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        else:
            logger.info(f"Creating new Delta Lake table at {full_write_path}")
            df.write.format("delta").mode("append").save(full_write_path)


class DeltaLakeMergeWriterFactory:
    """
    A factory class that creates instances of the `DeltaLakeMergeWriter` class.

    The `DeltaLakeMergeWriterFactory` class is responsible for creating instances of
    the `DeltaLakeMergeWriter` class, which is a concrete implementation of the `AbstractFileWriter` interface.
    It takes a `write_config` dictionary as input and returns a `DeltaLakeMergeWriter` instance with the appropriate configuration.

    The `get_writer` method is the main entry point for this factory class. It takes a `write_config`
    dictionary as input and returns a `DeltaLakeMergeWriter` instance with the configuration values
    extracted from the `write_config` dictionary.
    """

    def get_writer(self, write_config: dict) -> DeltaLakeMergeWriter:

        return DeltaLakeMergeWriter(
            write_path=write_config.get("write_path"),
            primary_key=write_config.get("primary_key"),
            table_name=write_config.get("table_name"),
        )


class FileWriterFactory:
    """
    A factory class that creates instances of the `AbstractFileWriter` interface.

    The `FileWriterFactory` class is responsible for creating instances of the `AbstractFileWriter`
    interface based on the `writer_type` specified in the `write_config` dictionary.

    The `get_writer` method is the main entry point for this factory class. It takes a `write_config` dictionary
    as input and returns an instance of the appropriate `AbstractFileWriter` implementation.
    """

    def get_writer(self, write_config: dict) -> AbstractFileWriter:

        writer_dict = {
            "delta_lake_merge": DeltaLakeMergeWriterFactory().get_writer(write_config)
        }

        return writer_dict[write_config.get("writer_type")]
