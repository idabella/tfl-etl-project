from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'tfl-etl',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

SPARK_SUBMIT = """
spark-submit \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.hadoop.fs.s3a.access.key=$AWS_ACCESS_KEY_ID \
  --conf spark.hadoop.fs.s3a.secret.key=$AWS_SECRET_ACCESS_KEY \
  --conf spark.hadoop.fs.s3a.endpoint=s3.eu-north-1.amazonaws.com \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.fast.upload=true \
  --conf spark.hadoop.fs.s3a.fast.upload.buffer=bytebuffer \
  --conf spark.sql.shuffle.partitions=4 \
  /opt/airflow/transformation/{script}
"""

with DAG(
    'tfl_etl_pipeline',
    default_args=default_args,
    description='TFL ETL Pipeline: Bronze -> Silver -> Gold',
    schedule_interval='0 * * * *',
    start_date=datetime(2026, 3, 1),
    catchup=False,
    tags=['tfl', 'etl'],
) as dag:

    # Silver Layer
    arrivals_silver = BashOperator(
        task_id='arrivals_silver',
        bash_command=SPARK_SUBMIT.format(script='silver/arrivals_silver.py'),
    )

    line_status_silver = BashOperator(
        task_id='line_status_silver',
        bash_command=SPARK_SUBMIT.format(script='silver/line_status_silver.py'),
    )

    bikepoint_silver = BashOperator(
        task_id='bikepoint_silver',
        bash_command=SPARK_SUBMIT.format(script='silver/bikepoint_silver.py'),
    )

    stoppoint_silver = BashOperator(
        task_id='stoppoint_silver',
        bash_command=SPARK_SUBMIT.format(script='silver/stoppoint_silver.py'),
    )

    airquality_silver = BashOperator(
        task_id='airquality_silver',
        bash_command=SPARK_SUBMIT.format(script='silver/airquality_silver.py'),
    )

    # Gold Dimensions
    dim_date = BashOperator(
        task_id='dim_date',
        bash_command=SPARK_SUBMIT.format(script='gold/dim_date.py'),
    )

    dim_time = BashOperator(
        task_id='dim_time',
        bash_command=SPARK_SUBMIT.format(script='gold/dim_time.py'),
    )

    dim_line = BashOperator(
        task_id='dim_line',
        bash_command=SPARK_SUBMIT.format(script='gold/dim_line.py'),
    )

    dim_station = BashOperator(
        task_id='dim_station',
        bash_command=SPARK_SUBMIT.format(script='gold/dim_station.py'),
    )

    dim_zone = BashOperator(
        task_id='dim_zone',
        bash_command=SPARK_SUBMIT.format(script='gold/dim_zone.py'),
    )

    dim_disruption = BashOperator(
        task_id='dim_disruption',
        bash_command=SPARK_SUBMIT.format(script='gold/dim_disruption.py'),
    )

    # Gold Facts
    fact_arrivals = BashOperator(
        task_id='fact_arrivals',
        bash_command=SPARK_SUBMIT.format(script='gold/fact_arrivals.py'),
    )

    fact_line_status = BashOperator(
        task_id='fact_line_status',
        bash_command=SPARK_SUBMIT.format(script='gold/fact_line_status.py'),
    )

    # Pipeline dependencies
    [arrivals_silver, line_status_silver, bikepoint_silver,
     stoppoint_silver, airquality_silver] >> dim_date

    [arrivals_silver, line_status_silver] >> dim_line
    [arrivals_silver, stoppoint_silver] >> dim_station
    stoppoint_silver >> dim_zone
    line_status_silver >> dim_disruption
    dim_time  # no upstream dependency

    [dim_date, dim_time, dim_line, dim_station] >> fact_arrivals
    [dim_date, dim_time, dim_line, dim_disruption] >> fact_line_status