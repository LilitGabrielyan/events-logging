package events.logging.analytics.aggregate

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.{nanvl, sum, when}
import org.apache.spark.{SparkConf, SparkContext}


/**
	* Collects data from hdfs and saves it in db for reporting purposes.
	*
	* @author lilit on 4/13/17.
	*/
object TrafficSummary {

	def main(args: Array[String]): Unit = {
		val config = args {
			0
		}
		val prop = new Properties()
		prop.load(getClass.getResourceAsStream("/config/config_" + config + ".properties"))
		val dbUrl = prop.getProperty("db.url")
		val dbUserName = prop.getProperty("db.username")
		val dbPassword = prop.getProperty("db.password")

		//		create context
		val conf = new SparkConf().setAppName("Calculate aggregated traffic data")
		val sparkContext = new SparkContext(conf)
		val sqlContext = new SQLContext(sparkContext)

		run(sqlContext, sparkContext.hadoopConfiguration, dbUrl, dbUserName, dbPassword)

	}

	def run(sqlContext: SQLContext, hadoopConfiguration: Configuration, dbUrl: String, dbUserName: String, dbPassword: String): Unit = {
		val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
		val cal = Calendar.getInstance()
		val now = dateFormat.format(cal.getTime)
		cal.add(Calendar.DATE, -1)
		val yesterday = dateFormat.format(cal.getTime)

		var mainEntryDF = Helper.getDataFrameIfExists(sqlContext, hadoopConfiguration, Helper.getMainEntryStruct,
			"events/DATA/ui/eventType=MainEntry/eventDate=" + now,
			"events/DATA/ui/eventType=MainEntry/eventDate=" + yesterday)

		var validateClickToGiveDF = Helper.getDataFrameIfExists(sqlContext, hadoopConfiguration, Helper.getValidateClickToGiveStruct,
			"events/DATA/ui/eventType=ValidateClickToGive/eventDate=" + now,
			"events/DATA/ui/eventType=ValidateClickToGive/eventDate=" + yesterday)

		var siteEntryDF = Helper.getDataFrameIfExists(sqlContext, hadoopConfiguration, Helper.getSiteEntryStruct,
			"events/DATA/ui/eventType=SiteEntry/eventDate=" + now,
			"events/DATA/ui/eventType=SiteEntry/eventDate=" + yesterday)

		var registrationDF = Helper.getDataFrameIfExists(sqlContext, hadoopConfiguration, Helper.getRegitrationStruct,
			"events/DATA/ui/eventType=Registration/eventDate=" + now,
			"events/DATA/ui/eventType=Registration/eventDate=" + yesterday)

		var registrationConfirmedDF = Helper.getDataFrameIfExists(sqlContext, hadoopConfiguration, Helper.getRegistrationConfirmedStruct,
			"events/DATA/ui/eventType=RegistrationConfirmed/eventDate=" + now,
			"events/DATA/ui/eventType=RegistrationConfirmed/eventDate=" + yesterday)

		mainEntryDF = mainEntryDF.groupBy("site", "app", "timestamp")
			.agg(sum(mainEntryDF("count")).as("session_count"))

		validateClickToGiveDF = validateClickToGiveDF
			.distinct()
			.groupBy("site", "app", "timestamp")
			.agg(sum(validateClickToGiveDF("count")).as("donation_count"))

		siteEntryDF = siteEntryDF
			.distinct()
			.groupBy("site", "app", "timestamp").agg(sum(siteEntryDF("count")).as("tab_session_count"))

		registrationDF = registrationDF
			.distinct()
			.groupBy("site", "app", "timestamp")
			.agg(sum(registrationDF("count")).as("registration_count"))

		registrationConfirmedDF = registrationConfirmedDF
			.distinct()
			.groupBy("site", "app", "timestamp").
			agg(sum(registrationConfirmedDF("count")).as("confirmed_registration_count"))

		var joinedDF = mainEntryDF.join(validateClickToGiveDF,
			mainEntryDF("site") === validateClickToGiveDF("site")
				&&
				mainEntryDF("app") === validateClickToGiveDF("app")
				&& mainEntryDF("timestamp") === validateClickToGiveDF("timestamp"),
			"outer"
		).select(nanvl(mainEntryDF("site"), validateClickToGiveDF("site")).as("site"),
			nanvl(mainEntryDF("app"), validateClickToGiveDF("app")).as("app"),
			when(mainEntryDF("timestamp").isNotNull, mainEntryDF("timestamp"))
				.otherwise(validateClickToGiveDF("timestamp")).as("timestamp"),
			mainEntryDF("session_count"),
			validateClickToGiveDF("donation_count")
		)
		joinedDF = joinedDF.join(siteEntryDF,
			joinedDF("site") === siteEntryDF("site")
				&&
				joinedDF("app") === siteEntryDF("app")
				&& joinedDF("timestamp") === siteEntryDF("timestamp"),
			"outer"
		).select(nanvl(joinedDF("site"), siteEntryDF("site")).as("site"),
			nanvl(joinedDF("app"), siteEntryDF("app")).as("app"),
			when(joinedDF("timestamp").isNotNull, joinedDF("timestamp"))
				.otherwise(siteEntryDF("timestamp")).as("timestamp"),
			joinedDF("session_count"),
			joinedDF("donation_count"),
			siteEntryDF("tab_session_count")
		)
		joinedDF = joinedDF.join(registrationDF,
			joinedDF("site") === registrationDF("site")
				&&
				joinedDF("app") === registrationDF("app")
				&& joinedDF("timestamp") === registrationDF("timestamp"),
			"outer"
		).select(nanvl(joinedDF("site"), registrationDF("site")).as("site"),
			nanvl(joinedDF("app"), registrationDF("app")).as("app"),
			when(joinedDF("timestamp").isNotNull, joinedDF("timestamp"))
				.otherwise(registrationDF("timestamp")).as("timestamp"),
			joinedDF("session_count"),
			joinedDF("donation_count"),
			joinedDF("tab_session_count"),
			registrationDF("registration_count")
		)
		joinedDF = joinedDF.join(registrationConfirmedDF,
			joinedDF("site") === registrationConfirmedDF("site")
				&&
				joinedDF("app") === registrationConfirmedDF("app")
				&& joinedDF("timestamp") === registrationConfirmedDF("timestamp"),
			"outer"
		).select(nanvl(joinedDF("site"), registrationConfirmedDF("site")).as("site_id"),
			nanvl(joinedDF("app"), registrationConfirmedDF("app")).as("application_id"),
			when(joinedDF("timestamp").isNotNull, joinedDF("timestamp"))
				.otherwise(registrationConfirmedDF("timestamp")).as("traffic_date"),
			joinedDF("session_count"),
			joinedDF("donation_count"),
			joinedDF("tab_session_count"),
			joinedDF("registration_count"),
			registrationConfirmedDF("confirmed_registration_count")
		)

		val dataSource = new DataSource(dbUrl, dbUserName, dbPassword, sqlContext)
		val keys = Array("application_id", "site_id", "traffic_date")
		val fields = Array("application_id", "site_id", "traffic_date", "session_count"
			, "donation_count", "registration_count", "tab_session_count", "confirmed_registration_count")

		dataSource.saveData(joinedDF, "traffic.daily_traffic_new", keys, fields)

	}

}