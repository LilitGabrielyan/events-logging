package events.logging.analytics.aggregate

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._

object Helper {


	def getMainEntryStruct: StructType = {
		StructType(Array(
			StructField("uuid", StringType),
			StructField("event", StringType),
			StructField("namespace", StringType),
			StructField("site", StringType),
			StructField("app", StringType),
			StructField("code", StringType),
			StructField("count", IntegerType),
			StructField("timestamp", DateType)
		))
	}

	def getValidateClickToGiveStruct: StructType = {
		StructType(Array(
			StructField("uuid", StringType),
			StructField("event", StringType),
			StructField("namespace", StringType),
			StructField("site", StringType),
			StructField("app", StringType),
			StructField("code", StringType),
			StructField("referer", StringType),
			StructField("count", IntegerType),
			StructField("timestamp", DateType),
			StructField("source", StringType)
		))
	}

	def getSiteEntryStruct: StructType = {
		StructType(Array(
			StructField("uuid", StringType),
			StructField("event", StringType),
			StructField("namespace", StringType),
			StructField("site", StringType),
			StructField("app", StringType),
			StructField("code", StringType),
			StructField("count", IntegerType),
			StructField("timestamp", DateType),
			StructField("source", StringType)
		))
	}

	def getRegitrationStruct: StructType = {
		StructType(Array(
			StructField("uuid", StringType),
			StructField("event", StringType),
			StructField("namespace", StringType),
			StructField("site", StringType),
			StructField("app", StringType),
			StructField("code", StringType),
			StructField("count", IntegerType),
			StructField("timestamp", DateType),
			StructField("source", StringType)
		))
	}

	def getRegistrationConfirmedStruct: StructType = {
		StructType(Array(
			StructField("uuid", StringType),
			StructField("event", StringType),
			StructField("namespace", StringType),
			StructField("site", StringType),
			StructField("app", StringType),
			StructField("code", StringType),
			StructField("count", IntegerType),
			StructField("timestamp", DateType),
			StructField("source", StringType)
		))
	}

	def getAdImpressionStruct: StructType = {
		StructType(Array(
			StructField("uuid", StringType),
			StructField("event", StringType),
			StructField("namespace", StringType),
			StructField("site", StringType),
			StructField("app", StringType),
			StructField("adId", StringType),
			StructField("placementId", StringType),
			StructField("count", IntegerType),
			StructField("timestamp", DateType),
			StructField("source", StringType)
		))
	}

	def getLinkFollowedStruct: StructType = {
		StructType(Array(
			StructField("uuid", StringType),
			StructField("event", StringType),
			StructField("namespace", StringType),
			StructField("site", StringType),
			StructField("app", StringType),
			StructField("code", StringType),
			StructField("count", IntegerType),
			StructField("timestamp", DateType),
			StructField("source", StringType)
		))
	}

	def getThirdPartyClickStruct: StructType = {
		StructType(Array(
			StructField("uuid", StringType),
			StructField("event", StringType),
			StructField("namespace", StringType),
			StructField("site", StringType),
			StructField("app", StringType),
			StructField("code", StringType),
			StructField("count", IntegerType),
			StructField("timestamp", DateType),
			StructField("source", StringType)
		))
	}

	def getDataFrameIfExists(sqlContext: org.apache.spark.sql.SQLContext, hadoopConfiguration: Configuration,
	                         schema: StructType, paths: String*): DataFrame = {
		val fs = FileSystem.get(hadoopConfiguration)
		val validPaths = paths.filter(p => fs.exists(new Path(p)))
		if (validPaths.isEmpty) {
			sqlContext.createDataFrame(sqlContext.sparkContext.emptyRDD[Row], schema)
		}
		else {
			sqlContext.read.parquet(validPaths.toArray: _*)
		}

	}

}