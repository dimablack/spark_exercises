"""
This module contains unit tests for the transformation steps of the ETL
job defined in wordcount.py. It makes use of a local version of PySpark
that is bundled with the PySpark package.
"""
import unittest
import warnings

from pyspark import SparkConf, SparkContext
from etl.wordcount import split_by_words, filter_by_word_length, filter_word_start_with, get_greater_than, sum_each_word


class SparkWordCountTests(unittest.TestCase):
	spark = None
	
	@classmethod
	def setUpClass(cls):
		warnings.filterwarnings(action='ignore', category=ResourceWarning)
		
		config = (SparkConf()
		          .setAppName('Unit test')
		          .setMaster('local[*]'))
		
		cls.spark = SparkContext.getOrCreate(config)
		
		cls.input = ["""
			lorem ipsum dolor sit amet lorem lorem dolor lorem sit lorem ipsum ipsum ipsum dolor
		"""]
	
	@classmethod
	def tearDownClass(cls):
		cls.spark.stop()
	
	def test_split_by_words(self):
		### Arrange
		input_rdd = self.spark.parallelize(self.input)
		expected = self.spark.parallelize([('lorem'), ('ipsum'), ('dolor'), ('sit')]).collect()
		
		### Act
		actual = split_by_words(input_rdd)
		
		### Assert
		result = actual.take(4)
		self.assertTrue([word in expected for word in result])
		self.assertEqual(expected[2], result[2])
	
	def test_filter_by_word_length(self):
		### Arrange
		input_rdd = self.spark.parallelize(self.input)
		expected = self.spark.parallelize([('lorem'), ('ipsum'), ('dolor'), ('amet')]).collect()
		
		### Act
		actual = split_by_words(input_rdd)
		actual = filter_by_word_length(actual, 4)
		
		### Assert
		result = actual.take(4)
		self.assertTrue([word in expected for word in result])
		self.assertEqual(expected[2], result[2])
	
	def test_filter_word_start_with(self):
		### Arrange
		input_rdd = self.spark.parallelize(self.input)
		expected = self.spark.parallelize([('sit'), ('sit')]).collect()
		
		### Act
		actual = split_by_words(input_rdd)
		actual = filter_word_start_with(actual, 'si')
		
		### Assert
		result = actual.collect()
		self.assertTrue([word in expected for word in result])
		self.assertEqual(expected[1], result[1])
	
	def test_sum_each_word(self):
		### Arrange
		input_rdd = self.spark.parallelize(self.input)
		expected = self.spark.parallelize([('lorem', 5), ('ipsum', 4), ('dolor', 3)]).collect()
		
		### Act
		actual = split_by_words(input_rdd)
		actual = sum_each_word(actual)
		
		### Assert
		result = actual.sortBy(lambda x: x[1], ascending=False).take(3)
		self.assertTrue([word in expected for word in result])
		self.assertEqual(expected[1][1], result[1][1])
	
	def test_get_greater_than(self):
		### Arrange
		input_rdd = self.spark.parallelize(self.input)
		expected = self.spark.parallelize([('lorem', 5), ('ipsum', 4)]).collect()
		
		### Act
		actual = split_by_words(input_rdd)
		actual = sum_each_word(actual)
		actual = get_greater_than(actual, 60)
		
		### Assert
		result = actual.sortBy(lambda x: x[1], ascending=False).take(3)
		self.assertTrue([word in expected for word in result])
		self.assertEqual(expected[0][1], result[0][1])


if __name__ == '__main__':
	unittest.main()
