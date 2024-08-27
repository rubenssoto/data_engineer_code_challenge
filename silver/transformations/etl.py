from infrastructure.config import Config
from common.session import get_spark_session
import logging

logger = logging.getLogger()


class Etl:
    """
    Runs the ETL (Extract, Transform, Load) process for the data pipeline.

    The `Etl` class is responsible for orchestrating the ETL process. It reads the input data,
    applies the configured transformations, and writes the transformed data to the output.

    The class takes a `Config` object as input, which provides the necessary configuration parameters for
    the ETL process, such as the file reader, transformers, and file writer.

    The `run()` method is the main entry point for the ETL process. It performs the following steps:
    1. Retrieves a Spark session using the `get_spark_session()` function.
    2. Reads the input data using the configured file reader.
    3. Applies the configured transformations to the data.
    4. Writes the transformed data to the output using the configured file writer.
    5. Moves the processed files using the configured checkpoint.
    """

    def __init__(self, config: Config):
        self.config = config

    def run(self):

        logger.info("Starting ETL process")
        spark = get_spark_session()

        logger.info("Reading input data")
        df = self.config.file_reader.read(spark=spark)

        logger.info("Applying transformations")
        for transformer in self.config.transformers:
            df = transformer.transform(df=df)

        logger.info("Writing output data")
        self.config.file_writer.write(df=df, spark=spark)

        self.config.checkpoint.move_files()
        logger.info("ETL process completed")
