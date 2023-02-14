"""
This module contains an example Spark ETL job definition.

It can be submitted to a Spark cluster (or locally) using the 'spark-submit'
command found in the '/bin' directory of all Spark distributions
(necessary for running any Spark job, locally or otherwise). For
example, this example script can be executed as follows,

bash -c "./spark/bin/spark-submit --master=local[2] --driver-memory 4096M --py-files ./packages.zip etl/spark_sql.py"

where packages.zip contains modules required by ETL job (in
this sample it contains a class to provide access to Log4j),
which need to be made available to each executor process on every node
in the cluster.
"""

from typing import Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from common.spark import real_start_spark_local


def main() -> None:
	# start Spark application and get Spark session, logger and config
	spark, logger, config = real_start_spark_local(
		number_cores=1,
		app_name='Spark SQL',
		files=["./configs/spark_sql_cfg.json"],
		# override default hostname taken from system
		spark_config={
			"spark.driver.host": "localhost",
			"spark.sql.legacy.timeParserPolicy": "LEGACY"
		})
	logger.info("Spark SQL Job is running")
	
	data = extract_data(spark, config)
	criticality_limit = config['criticality_limit'] / 100
	data_transformed = transform_data(spark, data[0], data[1], criticality_limit)
	
	load_data(data_transformed, config)
	
	# log the success and terminate Spark application
	logger.info('Word Count is finished')
	
	spark.stop()


def transform_data(spark: SparkSession, df_register: DataFrame, df_station: DataFrame,
                   criticality_limit: float) -> DataFrame:
	df_register = df_register.withColumn(
		'used_slots',
		F.when(F.col('used_slots').cast("int").isNotNull(), F.col('used_slots'))
		.otherwise(0)
	).withColumn(
		'free_slots',
		F.when(F.col('free_slots').cast("int").isNotNull(), F.col('free_slots'))
		.otherwise(0)
	) \
		.withColumn(
		"dayofweek",
		F.date_format('timestamp', 'E')
	) \
		.withColumn(
		"number_dayofweek",
		F.expr('weekday(timestamp) + 1')
	) \
		.withColumn(
		"hour",
		F.date_format('timestamp', 'H')
	)
	df_register = df_register.groupBy('station', 'dayofweek', 'number_dayofweek', 'hour').agg(
		F.count('*').alias("total_rows"),
		F.sum(F.when(F.col('free_slots') == 0, 1).otherwise(0)).alias("busy_hours"),
	)
	df_register = df_register.withColumn(
		"criticality",
		F.col('busy_hours') / F.col('total_rows')
	)
	result = df_register.join(
		df_station, df_register.station == df_station.id,
		'leftouter'
	).drop('id', 'name')
	# .where(F.col('criticality') >= criticality_limit)
	
	result.createOrReplaceTempView('results')
	result = spark.sql(f"SELECT * FROM results WHERE criticality >= {criticality_limit}")
	
	result = result.orderBy(
		F.desc('criticality'),
		F.asc('station'),
		F.asc('number_dayofweek'),
		F.asc('hour')
	)
	result = result.drop('total_rows', 'busy_hours', 'number_dayofweek')
	
	return result


def extract_data(spark: SparkSession, config: dict) -> Tuple[DataFrame, DataFrame]:
	"""
	Table name is hardcoded
	"""
	df_register = (
		spark
		.read
		.load(
			config['input_register'],
			format='csv',
			header=True,
			inferSchema=True,
			delimiter="\t"
		)
	)
	df_stations = (
		spark
		.read
		.load(
			config['input_stations'],
			format='csv',
			header=True,
			delimiter="\t"
		)
	)
	
	return df_register, df_stations


def load_data(results: DataFrame, config: dict) -> None:
	results.show(config['display_amount'])
	(
		results
		.write
		.option("header", True)
		.option("delimiter", ",")
		.mode('overwrite')
		.csv(config['output_results'])
	)


if __name__ == '__main__':
	main()
