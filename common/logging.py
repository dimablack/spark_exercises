from typing import Optional

from pyspark import SparkContext


class Log4py:
	
	def __init__(self, spark: SparkContext, custom_prefix: Optional[str] = "") -> None:
		config = spark.getConf()
		if not custom_prefix:
			app_id = config.get('spark.app.id')
			app_name = config.get('spark.app.name')
			custom_prefix = f"{app_name} {app_id}"
			
		log4j = spark._jvm.org.apache.log4j
		self._logger = log4j.LogManager.getLogger(custom_prefix)
		
	def error(self, msg: str) -> None:
		self._logger.error(msg)
		
	def info(self, msg: str) -> None:
		self._logger.info(msg)
		