from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import when, col, hour


def add_part_of_day_column(column_name: str, new_column_name: str) -> DataFrame:
    def _(df: DataFrame):

        return df.withColumn(
            new_column_name,
            when(
                (hour(col(column_name)) >= 0) & (hour(col(column_name)) < 12), "morning"
            )
            .when(
                (hour(col(column_name)) >= 12) & (hour(col(column_name)) < 17),
                "afternoon",
            )
            .otherwise("evening"),
        )

    return _


def platform_aggregation(df: DataFrame) -> DataFrame:

    return df.groupBy("platform", "part_of_the_day").count().withColumnRenamed("count", "users_count")


def most_viewed_courses(df: DataFrame) -> DataFrame:

    return df.groupBy("course_id", "part_of_the_day").count().withColumnRenamed("count", "views_count")
