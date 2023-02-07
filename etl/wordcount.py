"""
This module contains an example Spark ETL job definition.

It can be submitted to a Spark cluster (or locally) using the 'spark-submit'
command found in the '/bin' directory of all Spark distributions
(necessary for running any Spark job, locally or otherwise). For
example, this example script can be executed as follows,

bash -c "./spark/bin/spark-submit --master=local[2] --driver-memory 4096M --py-files ./packages.zip etl/wordcount.py --input_path ./input/sample.txt"

where packages.zip contains modules required by ETL job (in
this sample it contains a class to provide access to Log4j),
which need to be made available to each executor process on every node
in the cluster.
"""

import argparse
import os
import re
import shutil
from operator import add

from pyspark import SparkContext, RDD, SparkFiles

from common.spark import start_spark


def main() -> None:
	
	spark, logger = start_spark(
		app_name='Word Count',
		master='spark://localhost:7077',
		spark_config={
			"spark.driver.host": "localhost"
		})

	logger.info("Word Count is running")

	args = parse_args()
	
	letters = args.start_with
	letter_limit = args.letter_limit if args.letter_limit is not None else 0
	greater_than = args.greater_than
	input_path = args.input_path
	url = args.url
	
	data = extract_data(spark, input_path, url)
	data_transformed = transform_data(
		data,
		letters=letters,
		greater_than=greater_than,
		limit=letter_limit
	)
	
	# exit()
	load_data(data_transformed, greater_than)
	
	# log the success and terminate Spark application
	logger.info('Word Count is finished')
	
	spark.stop()


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(description='Important job arguments')
	parser.add_argument('--input_path', type=str, required=False, dest='input_path',
	                    help='Path to the input file for word count')
	parser.add_argument('--url', type=str, required=False, dest='url',
	                    help='Url to the input file for word count')
	parser.add_argument('--start_with', type=str, required=False, dest='start_with',
	                    help='Filter: Start word with letters')
	parser.add_argument('--letter_limit', type=int, required=False, dest='letter_limit',
	                    help='Filter: Count words where length is grater than limit(by default 0)')
	parser.add_argument('--greater_than', type=int, required=False, dest='greater_than',
	                    help='Filter: Word quantity greater than of the maximum quantity in this sample')
	
	return parser.parse_args()


def extract_data(spark: SparkContext, input_path: str, url: str = None) -> RDD:
	if url is not None:
		spark.addFile(url)
		_, filename = os.path.split(url)
		input_path = f"file://{SparkFiles.get(filename)}"
	lines = spark.textFile(input_path)
	return lines


def transform_data(lines: RDD, letters: str = None, greater_than: int = None, limit: int = 0) -> RDD:
	# Filter only words
	lines = lines \
		.flatMap(lambda line: line.split(" ")) \
		.filter(lambda word: re.search('[a-zA-Z]', word) is not None) \
		.map(lambda word: re.sub('[^0-9a-zA-Z]+', '', word))
	
	# Filter start word with letters
	if letters is not None:
		letters = letters.lower()
		lL = len(letters)
		lines = lines \
			.filter(lambda word: word[0:lL].lower().strip() == letters if len(word) >= lL else False)
	
	# Filter count word letter
	if limit > 0:
		lines = lines \
			.flatMap(lambda line: re.split('\W+', line.lower().strip())) \
			.filter(lambda x: len(x) >= limit)
	
	lines = lines.map(lambda w: (w, 1)) \
		.reduceByKey(add)
	
	# Filter word quantity greater than of the maximum quantity in this sample
	if greater_than is not None:
		max_word_count = (
			lines
			.map(lambda x: (x[1], x[0]))
			.sortByKey(False)
			.take(1)
		)
		if len(max_word_count) == 1:
			max_word_count = max_word_count[0][0]
			lines = lines.filter(lambda el: (el[1] / max_word_count) > (greater_than * .01))
	
	return lines


def load_data(counts: RDD, greater_than: int = None) -> None:
	# Save to file
	if greater_than is not None:
		# Remove output dir before doing anything
		outpath = '../output/wordcount'
		if os.path.exists(outpath) and os.path.isdir(outpath):
			shutil.rmtree(outpath)
		data_to_save = counts\
			.map(lambda x: x[0])
		data_to_save.saveAsTextFile(outpath)
	
	# Print result
	words = (
		counts
		.map(lambda x: (x[1], x[0]))
		.sortByKey(False)
		.take(100)
	)
	print(words)


if __name__ == '__main__':
	main()
