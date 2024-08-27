from abc import ABC, abstractmethod
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
import logging

logger = logging.getLogger()


class AbstractFileReader(ABC):
    """
    Abstract base class for file readers. Provides a common interface for reading files from a given source path.
    """

    def __init__(self, source_path: str):
        self.source_path = source_path

    @abstractmethod
    def read(self, spark: SparkSession) -> DataFrame:
        pass


class CsvFileReader(AbstractFileReader):

    def __init__(self, source_path: str):
        super().__init__(source_path)

    def read(self, spark: SparkSession) -> DataFrame:
        """
        Reads the CSV file located at the specified source path and returns a Spark DataFrame.

        Args:
            spark (SparkSession): The Spark session to use for reading the CSV file.

        Returns:
            DataFrame: A Spark DataFrame containing the data from the CSV file.
        """
        logger.info(f"Reading csv file from {self.source_path}")
        return spark.read.csv(self.source_path, header=True)

class DeltaLakeFileReader(AbstractFileReader):

    def __init__(self, source_path: str):
        super().__init__(source_path)

    def read(self, spark: SparkSession) -> DataFrame:

        return spark.read.format("delta").load(self.source_path)
    
class FileReaderFactory:
    """
    Factory class that returns the appropriate file reader based on the file type.
    """

    def get_reader(self, file_type: str, source_path: str) -> AbstractFileReader:
        """
        Args:
            file_type (str): The type of file to read (e.g. "csv").
            source_path (str): The path to the file to be read.

        Returns:
            AbstractFileReader: An instance of the appropriate file reader for the given file type.
        """

        file_reader_dict = {"csv": CsvFileReader,
                            "delta": DeltaLakeFileReader}

        return file_reader_dict[file_type](source_path=source_path)
