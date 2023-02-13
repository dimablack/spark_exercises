"""
This module contains an example Spark ETL job definition.

It can be submitted to a Spark cluster (or locally) using the 'spark-submit'
command found in the '/bin' directory of all Spark distributions
(necessary for running any Spark job, locally or otherwise). For
example, this example script can be executed as follows,

bash -c "./spark/bin/spark-submit --master=local[2] --driver-memory 4096M --py-files ./packages.zip etl/top_k.py --input_path ./input/Sample.csv"

where packages.zip contains modules required by ETL job (in
this sample it contains a class to provide access to Log4j),
which need to be made available to each executor process on every node
in the cluster.
"""

import argparse
import os
import shutil
from operator import add

from pyspark import SparkContext, RDD, SparkFiles
from itertools import combinations
from common.spark import start_spark


def main() -> None:
	spark, logger = start_spark(
		app_name='Top K Count',
		master='spark://localhost:7077',
		spark_config={
			"spark.driver.host": "localhost"
		})
	
	logger.info("Top K Count is running")
	
	args = parse_args()
	
	url = args.url
	input_path = args.input_path
	# input_path = '../input/Sample.csv'
	# url = None
	data = extract_data(spark, input_path, url)
	
	data_transformed = transform_data(data)
	
	# exit()
	load_data(spark, data_transformed)
	
	# log the success and terminate Spark application
	logger.info('Top K Count is finished')
	
	spark.stop()


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(description='Important job arguments')
	parser.add_argument('--input_path', type=str, required=False, dest='input_path',
	                    help='Path to the input file for top k count')
	parser.add_argument('--url', type=str, required=False, dest='url',
	                    help='Url to the input file for top k count')
	
	return parser.parse_args()


def extract_data(spark: SparkContext, input_path: str, url: str = None) -> RDD:
	lines = spark.textFile(input_path)
	return lines


def transform_data(lines: RDD) -> RDD:
	# parse csv take only
	lines = lines.map(lambda row: row.split(',')) \
		.map(lambda row: (row[2], row[1]))
	
	header = lines.first()
	# remove header
	lines = lines.filter(lambda row: header != row)
	# group products by userId
	# lines = lines.groupByKey().mapValues(set)
	lines = lines.distinct()
	# get only products tuple
	lines = lines.map(lambda row: tuple(row[1]))
	# filtering single product buying
	lines = lines.filter(lambda row: len(row) >= 2)
	# create all unique pairs of product from tuple
	lines = lines.map(lambda row: list(comb for comb in combinations(row, 2)))
	# split list of list to lists of tuples
	lines = lines.flatMap(lambda x: x)
	# count all pairs and sort by descending
	lines = (lines
	         .map(lambda x: ((x[0], x[1]), 1) if x[0] < x[1] else ((x[1], x[0]), 1))
	         .reduceByKey(add)
	         .sortBy(lambda x: x[1], ascending=False)
	         )
	# filter pair where count = 1
	lines = lines.filter(lambda x: x[1] > 1)
	
	return lines


def load_data(spark: SparkContext, counts: RDD) -> None:
	# Save to file
	outpath = './output/top_k'
	if os.path.exists(outpath) and os.path.isdir(outpath):
		shutil.rmtree(outpath)
	data_to_save = spark.parallelize(counts.take(100))
	data_to_save.saveAsTextFile(outpath)
	
	# Print result
	lines = (
		counts
		.take(100)
	)
	print(lines)


if __name__ == '__main__':
	main()
