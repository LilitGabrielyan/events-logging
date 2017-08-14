package events.logging.analytics.test

import java.nio.file.Files

import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{ClockWrapper, Duration, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, Suite}
import play.api.db.evolutions.{Evolution, Evolutions, SimpleEvolutionsReader}
import play.api.db.{Database, Databases}

import scala.reflect.io.Path
import scala.util.Try

/**
	* @author lilit on 4/14/17.
	*/
trait SparkStreamingSpec extends BeforeAndAfter with BeforeAndAfterAll {
	this: Suite =>
	private var _sc: SparkContext = _
	private var _ssc: StreamingContext = _
	private var _sqlc: SQLContext = _

	def batchDuration: Duration = Seconds(1)

	def checkpointDir: String = Files.createTempDirectory(this.getClass.getSimpleName).toUri.toString

	def sparkConfig: Map[String, String] = Map.empty

	def sc: SparkContext = _sc

	def ssc: StreamingContext = _ssc

	def sqlc: SQLContext = _sqlc

	var clock: ClockWrapper = _
	var database: Database = _

	before {
		val conf = new SparkConf()
			.setMaster("local[*]")
			.setAppName(this.getClass.getSimpleName)
			.set("spark.streaming.clock", "org.apache.spark.streaming.util.ManualClock")

		sparkConfig.foreach { case (k, v) => conf.setIfMissing(k, v) }

		_sc = new SparkContext(conf)
		_sqlc = new SQLContext(_sc)
		_ssc = new StreamingContext(sc, batchDuration)
		clock = new ClockWrapper(_ssc)
		_ssc.checkpoint(checkpointDir)

		//init db
		database = Databases.inMemory(
			name = "testdb"
		)

		val evolutions = Seq(
			Evolution(
				1,
				"CREATE SCHEMA traffic",
				"drop schema traffic;"
			), Evolution(
				2,
				"create table traffic.daily_traffic_new (application_id Integer, " +
					"site_id Integer, " +
					"session_count Integer, " +
					"tab_session_count Integer, " +
					"donation_count Integer, " +
					"registration_count Integer, " +
					"confirmed_registration_count Integer, " +
					"record_type_id Integer, " +
					"traffic_date Date" +
					");",
				"drop table traffic.daily_traffic_new;"
			),
			Evolution(
				3,
				"create table traffic.daily_ad_traffic (" +
					"application_id Integer, " +
					"site_id Integer, " +
					"content_ad_placement_id Integer, " +
					"impression_count Integer, " +
					"click_count Integer, " +
					"record_type_id Integer, " +
					"traffic_date Date" +
					");",
				"drop table traffic.daily_ad_traffic;"
			),
			Evolution(
				4,
				"create table traffic.daily_links_followed (" +
					"application_id Integer, " +
					"site_id Integer, " +
					"click_count Integer, " +
					"record_type_id Integer, " +
					"origin_code VARCHAR, " +
					"traffic_date Date" +
					");",
				"drop table traffic.daily_links_followed;"
			),
			Evolution(
				5,
				"create table traffic.daily_third_party_clicks (" +
					"application_id Integer, " +
					"site_id Integer, " +
					"click_count Integer, " +
					"record_type_id Integer, " +
					"origin_code VARCHAR, " +
					"traffic_date Date" +
					");",
				"drop table traffic.daily_third_party_clicks;"
			),
			Evolution(
				6,
				"create table traffic.daily_ad_rotation (" +
					"application_id Integer, " +
					"site_id Integer, " +
					"placement_id Integer, " +
					"ad_id Integer, " +
					"impression_count Integer, " +
					"click_count Integer, " +
					"record_type_id Integer, " +
					"traffic_date Date" +
					");",
				"drop table traffic.daily_ad_rotation;"
			)
		)
		Evolutions.applyEvolutions(database, new SimpleEvolutionsReader(Map("testdb" -> evolutions)))

	}


	after {
		if (_ssc != null) {
			_ssc.stop()
			_ssc = null
		}
		if (_sc != null) {
			_sc.stop()
			_sc = null
		}
	}

	override def afterAll {
		Try(Path("events").deleteRecursively)
	}

}
