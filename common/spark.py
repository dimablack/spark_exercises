from os import listdir, path
import json
from common import logging
from typing import Optional, Tuple

from pyspark import SparkContext, SparkConf, SparkFiles
from pyspark.sql import SparkSession


def start_spark_local(
		number_cores: int = 2,
		memory_gb: int = 4,
		app_name: Optional[str] = 'sample_job',
		jar_packages=[],
		files=[],
		spark_config = {},
		master: Optional[str] = None
) -> Tuple[SparkContext, logging.Log4py]:
	if not master:
		master = f"local[{number_cores}]"
	spark_config.update({"spark.driver.memory": f"{memory_gb}g"})
	return start_spark(
		app_name=app_name,
		master=master,
		jar_packages=jar_packages,
		files=files,
		spark_config=spark_config,
	)


def start_spark(
		app_name: Optional[str] = 'sample_job',
		master: Optional[str] = 'local[*]',
		jar_packages=[],
		files=[],
		spark_config={},
) -> Tuple[SparkContext, logging.Log4py]:
	"""
    Start Spark Context on the worker node, get logger and load config if present.
    
    :param app_name: Name of Spark app.
    :param master: Cluster connection details .
    :param jar_packages: List of Spark JAR package names.
    :param files: List of files to send to Spark cluster (master and
        workers).
    :param spark_config: Dictionary of config key-value pairs.
    :return: A tuple of references to the Spark session and logger.
    """
	config = (SparkConf().setAppName(app_name).setMaster(master))
	config.set("spark.jars.packages", ",".join(jar_packages))
	config.set("spark.files", ",".join(files))
	
	for k, v in spark_config.items():
		config.set(k, v)
		
	sc = SparkContext.getOrCreate(conf=config)
	
	logger = logging.Log4py(sc)
	
	return sc, logger


def real_start_spark(
		app_name: Optional[str] = "sample_job",
		master: Optional[str] = "local[*]",
		jar_packages=[],
		files=[],
		spark_config={}
) -> Tuple[SparkSession, logging.Log4py, Optional[dict]]:
	"""
	Start Spark Session like a boss on the worker node, get logger and load config if present.

	:param app_name: Name of Spark app.
	:param master: Cluster connection details .
	:param jar_packages: List of Spark JAR package names.
	:param files: List of files to send to Spark cluster (master and
		workers).
	:param spark_config: Dictionary of config key-value pairs.
	:return: A tuple of references to the Spark session and logger.
	"""
	spark_builder = (
		SparkSession
		.builder
		.master(master)
		.appName(app_name)
	)
	
	spark_builder.config("spark.jars.packages", ",".join(jar_packages))
	spark_builder.config("spark.files", ",".join(files))
	
	for k, v in spark_config.items():
		spark_builder.config(k, v)
	
	# Create a Spark Session object
	spark = spark_builder.getOrCreate()
	logger = logging.Log4py(spark.sparkContext)
	
	# get config file if sent to cluster with --files
	files_dir = SparkFiles.getRootDirectory()
	config_files = [filename
	                for filename in listdir(files_dir)
	                if filename.endswith('_cfg.json')]
	
	if config_files:
		path_to_file = path.join(files_dir, config_files[0])
		with open(path_to_file, 'r') as config_file:
			config_dict = json.load(config_file)
		logger.info(f"loaded configuration from {config_files[0]}")
	else:
		logger.info("no config provided")
		config_dict = None
	
	return spark, logger, config_dict
