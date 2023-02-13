"""
This module contains unit tests for the transformation steps of the ETL
job defined in wordcount.py. It makes use of a local version of PySpark
that is bundled with the PySpark package.
"""
import os
import unittest
import csv

from pyspark import SparkConf, SparkContext
from etl.top_k import transform_data, extract_data


class SparkTopKTests(unittest.TestCase):
	input = None
	spark = None
	
	@classmethod
	def setUpClass(cls):
		config = (SparkConf()
		          .setAppName('Unit test')
		          .setMaster('spark://localhost:7077'))
		# .setMaster('local[2]'))
		config.set("spark.driver.host", "localhost")
		cls.spark = SparkContext.getOrCreate(config)
		
		header = ['Id', 'ProductId', 'UserId', 'Title']
		data = [
			['1', 'P1', 'U2', 'title1'],
			['2', 'P2', 'U2', 'title2'],
			['3', 'P1', 'U1', 'title3'],
			['4', 'P3', 'U1', 'title4'],
			['5', 'P2', 'U1', 'title5'],
			['6', 'P4', 'U3', 'title6'],
			['7', 'P1', 'U3', 'title7'],
			['8', 'P3', 'U3', 'title8'],
		]
		cls.input = 'test_top_k.csv'
		with open(cls.input, 'w', encoding='UTF8', newline='') as f:
			writer = csv.writer(f)
			
			# write the header
			writer.writerow(header)
			
			# write multiple rows
			writer.writerows(data)
	
	@classmethod
	def tearDownClass(cls):
		os.remove(cls.input)
		cls.spark.stop()
	
	def test_transform_data(self):
		### Arrange
		input_rdd = extract_data(self.spark, self.input)
		expected = self.spark.parallelize([(('P1', 'P2'), 2), (('P1', 'P3'), 2)]).collect()
		
		### Act
		actual = transform_data(input_rdd)
		
		### Assert
		result = actual.collect()
		self.assertTrue([word in expected for word in result])
		self.assertTrue(expected[1][0], result[1][0])


if __name__ == '__main__':
	unittest.main()
