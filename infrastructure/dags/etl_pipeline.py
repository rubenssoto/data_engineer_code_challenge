import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os
dag = DAG(
    dag_id = "ETL_PIPELINE",
    default_args = {
        "start_date": airflow.utils.dates.days_ago(1)
    },
    schedule_interval = "@daily"
)

start = PythonOperator(
    task_id="start",
    python_callable = lambda: print("Jobs started"),
    dag=dag
)

silver = SparkSubmitOperator(
    task_id="silver",
    conn_id="spark_conn",
    application="jobs/silver/main.py",
    py_files="jobs/common/common.zip,jobs/silver/infrastructure.zip,jobs/silver/transformations.zip",
    files="jobs/silver/yaml_config/table_data.yaml",
    conf={
    "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-spark_2.12:3.2.0",
    "spark.hadoop.fs.s3a.access.key": os.getenv("AWS_ACCESS_KEY_ID"),
    "spark.hadoop.fs.s3a.secret.key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
},
    dag=dag
)

users_platforms = SparkSubmitOperator(
    task_id="users_platforms",
    conn_id="spark_conn",
    application="jobs/gold/main.py",
    py_files="jobs/common/common.zip,jobs/gold/infrastructure.zip,jobs/gold/transformations.zip",
    files="jobs/gold/yaml_config/users_platforms.yaml",
    application_args=["users_platforms"],
    conf={
    "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-spark_2.12:3.2.0",
    "spark.hadoop.fs.s3a.access.key": os.getenv("AWS_ACCESS_KEY_ID"),
    "spark.hadoop.fs.s3a.secret.key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
},
    dag=dag
)

most_viewed_courses = SparkSubmitOperator(
    task_id="most_viewed_courses",
    conn_id="spark_conn",
    application="jobs/gold/main.py",
    py_files="jobs/common/common.zip,jobs/gold/infrastructure.zip,jobs/gold/transformations.zip",
    files="jobs/gold/yaml_config/most_viewed_courses.yaml",
    application_args=["most_viewed_courses"],
    conf={
    "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.3.4,io.delta:delta-spark_2.12:3.2.0",
    "spark.hadoop.fs.s3a.access.key": os.getenv("AWS_ACCESS_KEY_ID"),
    "spark.hadoop.fs.s3a.secret.key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
},
    dag=dag
)


end = PythonOperator(
    task_id="end",
    python_callable = lambda: print("Jobs completed successfully"),
    dag=dag
)

start >> silver >> [users_platforms, most_viewed_courses] >> end