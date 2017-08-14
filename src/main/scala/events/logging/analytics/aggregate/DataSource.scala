package events.logging.analytics.aggregate

import org.apache.spark.sql.{DataFrame, SQLContext}
import scalikejdbc.{AutoSession, ConnectionPool, DB, SQL}

import scala.collection.mutable


/**
	* @author Lilit Gabrielyan.
	*/
class DataSource(url: String, userName: String, password: String, sqlContext: SQLContext) extends Serializable {

	var existingKeys: Map[String, mutable.Set[String]] = Map()

	Class.forName("org.postgresql.Driver")

	ConnectionPool.singleton(url, userName, password)
	val session = AutoSession

	def saveData(df: DataFrame, tableName: String, keys: Array[String], fields: Seq[String]): Unit = {
		val currentTableKeys = loadExistingData(tableName, keys)
		val rows = df.rdd.collect
		println("There are " + rows.length + " rows")
		rows.foreach(row => {
			val data = row.getValuesMap[String](fields)
			var rowKey = ""
			keys.foreach(key => {
				rowKey = rowKey + row.getAs(key) + "_"
			})
			if (currentTableKeys.contains(rowKey)) {
				//update row
				var newValues = ""
				var currentValue = ""
				data.foreach(p => {
					currentValue = ""
					currentValue = currentValue + p._2
					if (currentValue.contains("-")) {
						currentValue = "'" + currentValue + "'"
					}
					newValues = newValues + " " + p._1 + " = " + currentValue + ","
				})
				newValues = newValues + " record_type_id = 1 "

				var whereString = " where "
				keys.foreach(key => {
					var value = row.getAs(key).toString
					if (value.contains("-")) {
						value = "'" + value + "'"
					}
					whereString = whereString + " " + key + " = " + value + " and "
				})
				whereString = whereString.dropRight(4)
				DB localTx { implicit session =>
					val sqlUpdateString = "update " + tableName + " set " + newValues + whereString
					println(sqlUpdateString)
					SQL(sqlUpdateString).update.apply()
				}
			}
			else {
				//insert row
				DB localTx { implicit session =>
					val values = data.values.mkString(";").split(";").map(value => {
						if (value.contains("-")) {
							"'" + value + "'"
						}
						else {
							value
						}
					})
					val sqlInsertString = "insert into " + tableName + "(" + data.keys.mkString(",") + " , record_type_id)" +
						" values (" + values.mkString(",") + " ,1)"
					println(sqlInsertString)
					SQL(sqlInsertString).update.apply()
				}
				currentTableKeys.add(rowKey)
			}
		})
	}

	def loadExistingData(tableName: String, keys: Array[String]): mutable.Set[String] = {
		val updatedKeys = mutable.Set[String]()
		if (!existingKeys.contains(tableName)) {
			var dbKeys: List[String] = List()
			val queryString = "select * from " + tableName
			println(queryString)
			dbKeys = DB readOnly { implicit session =>
				SQL(queryString).map(rs => {
					println(rs.toMap())
					var rowKey = ""
					keys.foreach(key => {
						rowKey = rowKey + rs.string(key) + "_"
					})
					rowKey
				}).list.apply()
			}

			dbKeys.foreach(k => updatedKeys.add(k))
			existingKeys += (tableName -> updatedKeys)
		}

		updatedKeys
	}

}
