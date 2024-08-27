from common.session import get_spark_session
from pyspark.sql import SparkSession
from transformations.metrics import add_part_of_day_column, platform_aggregation, most_viewed_courses, add_metadata
from infrastructure.config import Config


class EtlUsersPlatforms:

    def __init__(self, config: Config):
        self.config = config

    def run(self):

        spark = get_spark_session()
        df = self.config.file_reader.read(spark)

        df = df.transform(
            add_part_of_day_column(
                column_name="timestamp", new_column_name="part_of_the_day"
            )
        ).transform(platform_aggregation).transform(add_metadata)

        self.config.file_writer.write(df=df, spark=spark)

class EtlMostViewedCourses:

    def __init__(self, config: Config):
        self.config = config

    def run(self):
        spark = get_spark_session()
        df = self.config.file_reader.read(spark)

        df = df.transform(
            add_part_of_day_column(
                column_name="timestamp", new_column_name="part_of_the_day"
            )
        ).transform(most_viewed_courses).transform(add_metadata)

        self.config.file_writer.write(df=df, spark=spark)

class EtlFactory:

    def get_etl(self, config: Config):

        etl_dict = {
            "users_platforms": EtlUsersPlatforms,
            "most_viewed_courses": EtlMostViewedCourses
        }

        return etl_dict[config.etl_name](config=config)
