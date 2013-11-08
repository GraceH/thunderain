package thunderainproject.thunderain.example.cloudstone.output

import thunderainproject.thunderain.framework.output.{PrimitiveObjInspectorFactory, AbstractEventOutput}
import thunderainproject.thunderain.framework.Event

import org.apache.spark.streaming.DStream

import com.mongodb.casbah.Imports._
import scala.collection.mutable
import scala.transient
import java.net.{HttpURLConnection, URL}


class MongoDBOutput extends AbstractEventOutput{
  //TODO to load the outputFormat automatically from shark/hive tableInfo
  @transient lazy val outputFormat = mutable.LinkedHashMap (
    ("h_host_ip", "String"),
    ("h_data_type", "String"),
    ("h_data_source", "String") ,
    ("h_user", "String") ,
    ("h_tags", "String") ,
    ("h_time", "Date"),
    ("b_message", "String"),
    ("b_log_level", "String"),
    ("b_trace", "String"),
    ("b_module_name", "String"),
    ("b_others", "String"),
    ("b_pid", "String"),
    ("b_tid", "String"),
    ("b_thread_name", "String"),
    ("b_source_file", "String"),
    ("b_line_number", "String")
  )

  val mongoDB = System.getenv("MONGO_DB")
  val mongoAddress = System.getenv("MONGO_ADDRESSES")

  val mongoDBupdatesTrigger = System.getenv("MONGO_DB_UPDATES_TRIGGER")

  @transient lazy val mongoURL = MongoClientURI(mongoAddress)
  @transient lazy val mongoDBClientOnSlave = MongoClient(mongoURL).apply(mongoDB)
  @transient lazy val table = mongoDBClientOnSlave(outputName)

  def output(stream: DStream[_]): Unit = {
    //insert each row into mongoDBCollection
    stream.foreach(r => {
      val filteredRDD = r.filter(_.asInstanceOf[Event].keyMap.nonEmpty)
      filteredRDD.foreach(row => {
        val cells = new mutable.HashMap[String, Any]
        row.asInstanceOf[Event].keyMap.map(col => {
          cells(col._1) = PrimitiveObjInspectorFactory.stringObjConversion(col._2, outputFormat(col._1))
        })
        table.insert(MongoDBObject(cells.toList))
      })

      if(mongoDBupdatesTrigger != null) {
        val recordCnt = filteredRDD.count()
        if(recordCnt > 0){
          val url = "%s/%d".format(mongoDBupdatesTrigger, recordCnt)
          if(sendHTTPGetRequest(url) != 200)
            throw new Exception("Bad ack from %s".format(url))
        }
      }
    })

  }

  def sendHTTPGetRequest(url: String): Int = {
    val obj: URL = new URL(url)
    val con:HttpURLConnection = obj.openConnection().asInstanceOf[HttpURLConnection]
    con.getResponseCode()
  }
}