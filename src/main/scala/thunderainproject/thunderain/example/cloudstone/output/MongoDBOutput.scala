package thunderainproject.thunderain.example.cloudstone.output

import thunderainproject.thunderain.framework.output.{PrimitiveObjInspectorFactory, AbstractEventOutput}
import thunderainproject.thunderain.framework.Event

import org.apache.spark.streaming.DStream
import org.apache.spark.Logging

import com.mongodb.casbah.Imports._
import scala.collection.mutable
import scala.transient
import java.net.{HttpURLConnection, URL}

//singleton for mongoDB client which contains a connection pool (10 threads by default)
object MongoDBOutput{
  private var mongoDBClientOnSlave: MongoClient = _
  def getClientOnSlave(address: String): MongoClient = synchronized {
    if(mongoDBClientOnSlave == null) {
      val mongoURL  = MongoClientURI(address)
      mongoDBClientOnSlave = MongoClient(mongoURL)
    }
    //println("client:" + mongoDBClientOnSlave)
    mongoDBClientOnSlave
  }
}

class MongoDBOutput extends AbstractEventOutput with Logging{
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

  def output(stream: DStream[_]): Unit = {
    //insert each row into mongoDBCollection
    stream.foreach(r => {
      val filteredRDD = r.filter(_.asInstanceOf[Event].keyMap.nonEmpty)
      filteredRDD.foreachPartition((iter: Iterator[Any]) => {
        val db = MongoDBOutput.getClientOnSlave(mongoAddress).apply(mongoDB)
        val table = db(outputName)
        try {
          iter.foreach( row => {
            val cells = new mutable.HashMap[String, Any]
            row.asInstanceOf[Event].keyMap.map(col => {
              cells(col._1) = PrimitiveObjInspectorFactory.stringObjConversion(col._2, outputFormat(col._1))
            })
            //must have the WriteConcern option to ensure the performance
            table.insert(MongoDBObject(cells.toList), WriteConcern.None)
          })
          table.ensureIndex(MongoDBObject("h_time" -> 1))
          table.ensureIndex(MongoDBObject("h_time" -> -1))
        }
        catch {
          case _ => logError("Failed to insert DB")
        }
      })

      if(mongoDBupdatesTrigger != null) {
        val recordCnt = filteredRDD.count()
        if(recordCnt > 0){
          val url = "%s/%d".format(mongoDBupdatesTrigger, recordCnt)
          if(sendHTTPGetRequest(url) != 200)
            logError("Bad ack from %s".format(url))
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

