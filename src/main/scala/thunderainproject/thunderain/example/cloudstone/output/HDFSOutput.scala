package thunderainproject.thunderain.example.cloudstone.output

import thunderainproject.thunderain.framework.output.AbstractEventOutput
import thunderainproject.thunderain.framework.Event

import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable
import shark.SharkContext
import org.apache.spark.Logging


class HDFSOutput extends AbstractEventOutput with Logging{
  var fieldNames: Array[String] = _

  val hdfsPath = System.getenv("HDFS_PATH")
  val deliminator = "|"

  override def preprocessOutput(stream: DStream[_]): DStream[_] = {
    val sc = try{
      stream.context.sparkContext.asInstanceOf[SharkContext]  }
    catch {
      case _ => {
        logError("Failed to obtain a SharkContext instance")
        null
      }
    }
    if(sc != null){
      val resultSets = sc.sql("describe %s".format(outputName)).flatMap(_.split("\\t")).zipWithIndex
      fieldNames = resultSets.filter(_._2%3==0).map(_._1).toArray
    }
    //no transformation for the input stream here
    stream
  }

  def output(stream: DStream[_]): Unit = {
    val outputPath = hdfsPath + "/" + outputName
    stream.foreach( (rdd, t) => {
      val partitionPath = outputPath + "/" + t.milliseconds / 1000
      rdd.filter(_.asInstanceOf[Event].keyMap.nonEmpty).map(row => {
        val values = fieldNames.map(row.asInstanceOf[Event].keyMap(_))
        values.mkString(deliminator)
      }).saveAsTextFile(partitionPath)
    })
  }
}
