package events.logging.analytics.test

import java.sql.ResultSet
import java.text.SimpleDateFormat
import java.util.{Calendar, UUID}

import events.logging.analytics.aggregate.TrafficSummary
import events.logging.analytics.{AnalyticsLogGroups, AnalyticsStoreLogReader}
import org.apache.spark.rdd.RDD
import org.junit.runner.RunWith
import org.scalatest.concurrent.Eventually
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers, _}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps

/**
	* @author lilit on 4/14/17.
	*/
@RunWith(classOf[JUnitRunner])
class TestTrafficSummary extends FlatSpec with SparkStreamingSpec
	with GivenWhenThen with Matchers with Eventually {

	"Traffic summary app with only one event type" should " store data in db " in {
		val lines = mutable.Queue[RDD[String]]()
		val dstream = ssc.queueStream(lines)
		dstream.print()
		AnalyticsStoreLogReader.run(dstream, sqlc)
		ssc.start()
		val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
		val resultFormat = new SimpleDateFormat("yyyy-MM-dd")
		val cal = Calendar.getInstance()
		val now = dateFormat.format(cal.getTime)
		val resultNow = resultFormat.format(cal.getTime)
		cal.add(Calendar.DATE, -1)
		val yesterday = dateFormat.format(cal.getTime)
		val resultYesterday = resultFormat.format(cal.getTime)

		val expected = Map("20_3_" + resultNow -> 5, "20_3_" + resultYesterday -> 6)
		When("new messages are added to stream")

		lines += sc.makeRDD(Seq("{ \"uuid\":\"" + UUID.randomUUID() + "\", \"event\": \"MainEntry\", \"site\": 20, \"app\": 3, \"namespace\": \"default\", \"count\": 1, \"code\": 123, \"timestamp\": \"" + now + "\"     }",
			"{ \"uuid\":\"" + UUID.randomUUID() + "\", \"event\": \"MainEntry\", \"site\": 20, \"app\": 3, \"namespace\": \"default\", \"count\": 1, \"code\": 123, \"timestamp\": \"" + now + "\"     }",
			"{ \"uuid\":\"" + UUID.randomUUID() + "\", \"event\": \"MainEntry\", \"site\": 20, \"app\": 3, \"namespace\": \"default\", \"count\": 1, \"code\": 123, \"timestamp\": \"" + now + "\"     }",
			"{ \"uuid\":\"" + UUID.randomUUID() + "\", \"event\": \"MainEntry\", \"site\": 20, \"app\": 3, \"namespace\": \"default\", \"count\": 1, \"code\": 123, \"timestamp\": \"" + now + "\"     }",
			"{ \"uuid\":\"" + UUID.randomUUID() + "\", \"event\": \"MainEntry\", \"site\": 20, \"app\": 3, \"namespace\": \"default\", \"count\": 1, \"code\": 123, \"timestamp\": \"" + now + "\"     }",
			"{ \"uuid\":\"" + UUID.randomUUID() + "\", \"event\": \"MainEntry\", \"site\": 20, \"app\": 3, \"namespace\": \"default\", \"count\": 1, \"code\": 123, \"timestamp\": \"" + yesterday + "\"     }",
			"{ \"uuid\":\"" + UUID.randomUUID() + "\", \"event\": \"MainEntry\", \"site\": 20, \"app\": 3, \"namespace\": \"default\", \"count\": 1, \"code\": 123, \"timestamp\": \"" + yesterday + "\"     }",
			"{ \"uuid\":\"" + UUID.randomUUID() + "\", \"event\": \"MainEntry\", \"site\": 20, \"app\": 3, \"namespace\": \"default\", \"count\": 1, \"code\": 123, \"timestamp\": \"" + yesterday + "\"     }",
			"{ \"uuid\":\"" + UUID.randomUUID() + "\", \"event\": \"MainEntry\", \"site\": 20, \"app\": 3, \"namespace\": \"default\", \"count\": 1, \"code\": 123, \"timestamp\": \"" + yesterday + "\"     }",
			"{ \"uuid\":\"" + UUID.randomUUID() + "\", \"event\": \"MainEntry\", \"site\": 20, \"app\": 3, \"namespace\": \"default\", \"count\": 1, \"code\": 123, \"timestamp\": \"" + yesterday + "\"     }",
			"{ \"uuid\":\"" + UUID.randomUUID() + "\", \"event\": \"MainEntry\", \"site\": 20, \"app\": 3, \"namespace\": \"default\", \"count\": 1, \"code\": 123, \"timestamp\": \"" + yesterday + "\"     }"))

		clock.advance(1000)

		Then("Group stream data")
		Thread.sleep(3000)
		AnalyticsLogGroups.processUIEvents(sqlc, sc.hadoopConfiguration)
		Thread.sleep(1000)
		TrafficSummary.run(sqlc, sc.hadoopConfiguration, "jdbc:h2:mem:testdb", "", "")

		Then("Load data from db")
		val result = database.dataSource.getConnection.createStatement().executeQuery("select * from traffic.DAILY_TRAFFIC_NEW")
		var actual: Map[String, Int] = Map()
		while (result.next()) {
			val siteId = result.getInt("site_id")
			val applicationId = result.getInt("application_id")
			val trafficDate = result.getDate("traffic_date")
			val session_count = result.getInt("session_count")
			val tab_session_count = result.getInt("tab_session_count")
			val donation_count = result.getInt("donation_count")
			val registration_count = result.getInt("registration_count")
			val confirmed_registration_count = result.getInt("confirmed_registration_count")
			actual += (siteId + "_" + applicationId + "_" + trafficDate -> session_count)
		}
		eventually(timeout(12 seconds)) {

			actual shouldBe expected

		}

	}

	"Traffic summary app with all event types" should " store data in db " in {
		val lines = mutable.Queue[RDD[String]]()
		val dstream = ssc.queueStream(lines)
		dstream.print()
		AnalyticsStoreLogReader.run(dstream, sqlc)
		ssc.start()
		val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
		val resultFormat = new SimpleDateFormat("yyyy-MM-dd")
		val cal = Calendar.getInstance()
		val now = dateFormat.format(cal.getTime)
		val resultNow = resultFormat.format(cal.getTime)
		cal.add(Calendar.DATE, -1)
		val yesterday = dateFormat.format(cal.getTime)
		val resultYesterday = resultFormat.format(cal.getTime)

		val expected = Map("21_3_" + resultNow -> "2_2_4_2_4",
											"21_3_" + resultYesterday -> "4_2_2_2_2")
		When("new messages are added to stream")

		lines += sc.makeRDD(Seq("{ \"uuid\":\"" + UUID.randomUUID() + "\", \"event\": \"MainEntry\", \"site\": 21, \"app\": 3, \"namespace\": \"default\", \"count\": 2, \"code\": 123, \"timestamp\": \"" + now + "\"     }",
			"{ \"uuid\":\"" + UUID.randomUUID() + "\", \"event\": \"ValidateClickToGive\", \"site\": 21, \"app\": 3, \"namespace\": \"default\", \"count\": 2, \"code\": 123, \"timestamp\": \"" + now + "\"     }",
			"{ \"uuid\":\"" + UUID.randomUUID() + "\", \"event\": \"ValidateClickToGive\", \"site\": 21, \"app\": 3, \"namespace\": \"default\", \"count\": 2, \"code\": 123, \"timestamp\": \"" + now + "\"     }",
			"{ \"uuid\":\"" + UUID.randomUUID() + "\", \"event\": \"SiteEntry\", \"site\": 21, \"app\": 3, \"namespace\": \"default\", \"count\": 2, \"code\": 123, \"timestamp\": \"" + now + "\"     }",
			"{ \"uuid\":\"" + UUID.randomUUID() + "\", \"event\": \"Registration\", \"site\": 21, \"app\": 3, \"namespace\": \"default\", \"count\": 2, \"code\": 123, \"timestamp\": \"" + now + "\"     }",
			"{ \"uuid\":\"" + UUID.randomUUID() + "\", \"event\": \"RegistrationConfirmed\", \"site\": 21, \"app\": 3, \"namespace\": \"default\", \"count\": 2, \"code\": 123, \"timestamp\": \"" + now + "\"     }",
			"{ \"uuid\":\"" + UUID.randomUUID() + "\", \"event\": \"RegistrationConfirmed\", \"site\": 21, \"app\": 3, \"namespace\": \"default\", \"count\": 2, \"code\": 123, \"timestamp\": \"" + now + "\"     }",
			"{ \"uuid\":\"" + UUID.randomUUID() + "\", \"event\": \"MainEntry\", \"site\": 21, \"app\": 3, \"namespace\": \"default\", \"count\": 2, \"code\": 123, \"timestamp\": \"" + yesterday + "\"     }",
			"{ \"uuid\":\"" + UUID.randomUUID() + "\", \"event\": \"MainEntry\", \"site\": 21, \"app\": 3, \"namespace\": \"default\", \"count\": 2, \"code\": 123, \"timestamp\": \"" + yesterday + "\"     }",
			"{ \"uuid\":\"" + UUID.randomUUID() + "\", \"event\": \"ValidateClickToGive\", \"site\": 21, \"app\": 3, \"namespace\": \"default\", \"count\": 2, \"code\": 123, \"timestamp\": \"" + yesterday + "\"     }",
			"{ \"uuid\":\"" + UUID.randomUUID() + "\", \"event\": \"SiteEntry\", \"site\": 21, \"app\": 3, \"namespace\": \"default\", \"count\": 2, \"code\": 123, \"timestamp\": \"" + yesterday + "\"     }",
			"{ \"uuid\":\"" + UUID.randomUUID() + "\", \"event\": \"Registration\", \"site\": 21, \"app\": 3, \"namespace\": \"default\", \"count\": 2, \"code\": 123, \"timestamp\": \"" + yesterday + "\"     }",
			"{ \"uuid\":\"" + UUID.randomUUID() + "\", \"event\": \"RegistrationConfirmed\", \"site\": 21, \"app\": 3, \"namespace\": \"default\", \"count\": 2, \"code\": 123, \"timestamp\": \"" + yesterday + "\"     }"))

		clock.advance(1000)

		Then("Group stream data")
		Thread.sleep(3000)
		AnalyticsLogGroups.processUIEvents(sqlc, sc.hadoopConfiguration)
		Thread.sleep(1000)
		TrafficSummary.run(sqlc, sc.hadoopConfiguration, "jdbc:h2:mem:testdb", "", "")

		Then("Load data from db")
		val result = database.dataSource.getConnection.createStatement().executeQuery("select * from traffic.DAILY_TRAFFIC_NEW where site_id=21")
		var actual: Map[String, String] = Map()
		while (result.next()) {
			actual += (generateKey(result) -> generateValue(result))
		}
		eventually(timeout(12 seconds)) {

			actual shouldBe expected

		}

	}

	"Traffic summary app running multiple times without data added" should " not change db data" in {
		val lines = mutable.Queue[RDD[String]]()
		val dstream = ssc.queueStream(lines)
		dstream.print()
		AnalyticsStoreLogReader.run(dstream, sqlc)
		ssc.start()
		val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
		val resultFormat = new SimpleDateFormat("yyyy-MM-dd")
		val cal = Calendar.getInstance()
		val now = dateFormat.format(cal.getTime)
		val resultNow = resultFormat.format(cal.getTime)
		cal.add(Calendar.DATE, -1)
		val yesterday = dateFormat.format(cal.getTime)
		val resultYesterday = resultFormat.format(cal.getTime)

		val expected = Map("20_3_" + resultNow -> "5_0_0_0_0",
												"20_3_" + resultYesterday -> "6_0_0_0_0",
												"21_3_" + resultNow -> "2_2_4_2_4",
												"21_3_" + resultYesterday -> "4_2_2_2_2")
		When("no messages are added to stream")
		clock.advance(1000)

		Then("Group stream data")
		Thread.sleep(3000)
		AnalyticsLogGroups.processUIEvents(sqlc, sc.hadoopConfiguration)
		Thread.sleep(1000)
		TrafficSummary.run(sqlc, sc.hadoopConfiguration, "jdbc:h2:mem:testdb", "", "")

		Then("Load data from db")
		val result = database.dataSource.getConnection.createStatement().executeQuery("select * from traffic.DAILY_TRAFFIC_NEW")
		var actual: Map[String, String] = Map()
		while (result.next()) {
			actual += (generateKey(result) -> generateValue(result))
		}
		eventually(timeout(12 seconds)) {

			actual shouldBe expected

		}

	}

	def generateKey(row: ResultSet): String = {
		val siteId = row.getInt("site_id")
		val applicationId = row.getInt("application_id")
		val trafficDate = row.getDate("traffic_date")
		siteId + "_" + applicationId + "_" + trafficDate
	}

	def generateValue(row: ResultSet): String = {
		val session_count = row.getInt("session_count")
		val tab_session_count = row.getInt("tab_session_count")
		val donation_count = row.getInt("donation_count")
		val registration_count = row.getInt("registration_count")
		val confirmed_registration_count = row.getInt("confirmed_registration_count")
		session_count + "_" + tab_session_count + "_" + donation_count + "_" + registration_count + "_" + confirmed_registration_count
	}

}