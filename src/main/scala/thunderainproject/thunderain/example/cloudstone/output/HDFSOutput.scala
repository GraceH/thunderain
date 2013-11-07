package thunderainproject.thunderain.example.cloudstone.output

import thunderainproject.thunderain.framework.output.AbstractEventOutput
import thunderainproject.thunderain.framework.Event

import org.apache.spark.streaming.DStream

import scala.collection.mutable


class HDFSOutput extends AbstractEventOutput{
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

  val hdfsPath = System.getenv("HDFS_PATH")
  val deliminator = "|"

  def output(stream: DStream[_]): Unit = {
    val outputPath = hdfsPath + "/" + outputName
    stream.foreach( (rdd, t) => {
      val partitionPath = outputPath + "/" + t.milliseconds / 1000
      rdd.filter(_.asInstanceOf[Event].keyMap.nonEmpty).map(row => {
        val values = outputFormat.map( r => {
          row.asInstanceOf[Event].keyMap(r._1)
        })
        values.mkString(deliminator)
      }).saveAsTextFile(partitionPath)
    })
  }
}
