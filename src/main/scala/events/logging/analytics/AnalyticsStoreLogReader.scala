package events.logging.analytics

import java.util.Properties

import org.apache.spark.sql.types._
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
	* Processes date from kafka to HDFS.
	* @author lilit on 4/10/17.
	*/
object AnalyticsStoreLogReader {
	def main(args: Array[String]) {
		val config = args {
			0
		}
		val prop = new Properties()
		prop.load(getClass.getResourceAsStream("/config/config_" + config + ".properties"))

		val args2 = Array(prop.getProperty("zookeeper.url"), prop.getProperty("kafka.ui.group"), prop.getProperty("kafka.ui.topic"), "1")
		val Array(zkQuorum, group, topics, numThreads) = args2
		val conf = new SparkConf().setAppName("Store UI Event Writer")
		val sparkContext = new SparkContext(conf)


		val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)

		val ssc = new StreamingContext(sparkContext, Minutes(5))
		ssc.checkpoint("checkpointUI")

		val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
		val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
		lines.print()

		run(lines, sqlContext)

		ssc.start()
		ssc.awaitTermination()
	}

	def run(lines: DStream[String], sqlContext: SQLContext): Unit = {
		val schema = StructType(Array(
			StructField("event", StringType),
			StructField("namespace", StringType),
			StructField("host", StringType),
			StructField("path", StringType),
			StructField("method", StringType),
			StructField("site", StringType),
			StructField("app", StringType),
			StructField("code", StringType),
			StructField("referer", StringType),
			StructField("origin", StringType),
			StructField("page", IntegerType),
			StructField("adId", IntegerType),
			StructField("placementId", IntegerType),
			StructField("count", IntegerType),
			StructField("timestamp", DateType),
			StructField("source", StringType),
			StructField("uuid", StringType)
		))

		lines.foreachRDD(rdd => {
			if (!rdd.isEmpty()) {
				try {
					sqlContext.read.schema(schema)
						.json(rdd)
						.repartition(4)
						.write.mode(SaveMode.Append)
						.parquet("events/WAL/ui/" + System.currentTimeMillis() / 1000)
				}
				catch {
					case ex: Exception => println(ex)
				}
			}

		})

	}
}