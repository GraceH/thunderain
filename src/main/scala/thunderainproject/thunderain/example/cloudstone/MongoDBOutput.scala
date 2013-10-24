package thunderainproject.thunderain.example.cloudstone

import thunderainproject.thunderain.framework.output.{PrimitiveObjInspectorFactory, AbstractEventOutput}
import org.apache.spark.streaming.DStream

import com.mongodb.casbah.Imports._
import scala.collection.mutable
import thunderainproject.thunderain.framework.Event

class MongoDBOutput extends AbstractEventOutput{
  //TODO to load the outputFormat automatically from shark/hive tableInfo
  @transient lazy val outputFormat = mutable.LinkedHashMap (
    ("h_host_ip", "String"),
    ("h_data_type", "String"),
    ("h_data_source", "String") ,
    ("h_user", "String") ,
    ("h_tags", "String") ,
    ("h_time", "Long"),
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


  val mongoURL = MongoClientURI(System.getenv("MONGO_ADDRESSES"))
  val mongoDB = System.getenv("MONGO_DB")

  @transient lazy val mongoDBClientOnSlave = MongoClient(mongoURL).apply(mongoDB)


  /**
   * abstract method of output DStream, derivatives should implement this.
   */
  def output(stream: DStream[_]): Unit = {
    val table = mongoDBClientOnSlave(outputName)
    //insert each row into mongoDBCollection
    stream.map(row => {
      var builder = MongoDBObject.newBuilder
      row.asInstanceOf[Event].keyMap.map(col => {
        builder += col._1 ->  PrimitiveObjInspectorFactory.stringObjConversion(col._1, outputFormat(col._1))
      })
      table.insert(builder)
    }).foreach(_ => Unit)
  }
}
