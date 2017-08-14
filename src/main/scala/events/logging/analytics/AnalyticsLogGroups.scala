package events.logging.analytics

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.streaming.{Minutes, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
	* Processes date in HDFS into folders according to event types and date.
	*
	* @author lilit on 4/11/17.
	*/
object AnalyticsLogGroups {

	private val PROCESSED_PATH = "events/PROCESSED"

	private val WAL_PATH = "events/WAL"

	private val DATA_PATH = "events/DATA"

	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("Group events by date")
		val sparkContext = new SparkContext(conf)


		val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)

		val ssc = new StreamingContext(sparkContext, Minutes(5))
		ssc.checkpoint("checkpoint")


		processOrderEvents(sqlContext, sparkContext.hadoopConfiguration)

		processOrderEvents(sqlContext, sparkContext.hadoopConfiguration)

	}

	def processOrderEvents(sqlContext: SQLContext, hadoopConfiguration: Configuration): Unit = {
		val paths = FileUtils.succeedStreamDirs(WAL_PATH + "/orders", hadoopConfiguration)
		if (paths.length > 0) {
			println("grouping order data by date .....from" + paths.mkString(" "))
			val orderDf = sqlContext.read.parquet(paths: _*)
			orderDf.withColumn("orderDate", orderDf.col("createdAt")).repartition(1).write.mode(SaveMode.Append)
				.partitionBy("orderDate").parquet(DATA_PATH + "/orders")

			//move processed files
			println("moving order data to processed folder .....")
			FileUtils.moveProcessedFiles(paths, PROCESSED_PATH + "/orders", hadoopConfiguration)
		}
	}

	def processUIEvents(sqlContext: SQLContext, hadoopConfiguration: Configuration): Unit = {
		val uiPaths = FileUtils.succeedStreamDirs(WAL_PATH + "/ui", hadoopConfiguration)
		if (uiPaths.length > 0) {
			println("grouping ui data by date ..... from" + uiPaths.mkString(" "))
			val uiDF = sqlContext.read.parquet(uiPaths: _*)
			uiDF.withColumn("eventType", uiDF.col("event"))
				.withColumn("eventDate", uiDF.col("timestamp"))
				.repartition(1).write.mode(SaveMode.Append)
				.partitionBy("eventType", "eventDate").parquet(DATA_PATH + "/ui")
			//move processed files
			println("moving order data to processed folder .....")
			FileUtils.moveProcessedFiles(uiPaths, PROCESSED_PATH + "/ui", hadoopConfiguration)
		}

	}
}