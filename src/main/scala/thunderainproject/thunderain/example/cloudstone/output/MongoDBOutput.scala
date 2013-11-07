package thunderainproject.thunderain.example.cloudstone.output

import thunderainproject.thunderain.framework.output.{PrimitiveObjInspectorFactory, AbstractEventOutput}
import thunderainproject.thunderain.framework.Event

import org.apache.spark.streaming.DStream

import com.mongodb.casbah.Imports._
import scala.collection.mutable
import scala.transient
import shark.SharkContext


class MongoDBOutput extends AbstractEventOutput{
  var outputFormat: Map[String, String] = _

  val mongoDB = System.getenv("MONGO_DB")
  val mongoAddress = System.getenv("MONGO_ADDRESSES")

  @transient lazy val mongoURL = MongoClientURI(mongoAddress)
  @transient lazy val mongoDBClientOnSlave = MongoClient(mongoURL).apply(mongoDB)
  @transient lazy val table = mongoDBClientOnSlave(outputName)

  override def preprocessOutput(stream: DStream[_]): DStream[_] = {
    val sc = stream.context.sparkContext.asInstanceOf[SharkContext]
    val resultSets = sc.sql("describe %s".format(outputName))
      .map(x => {val y = x.split("\\t"); (y(0)-> y(1))})
    outputFormat = resultSets.toMap
    //no transformation for the input stream here
    stream
  }

  def output(stream: DStream[_]): Unit = {
    //insert each row into mongoDBCollection
    stream.foreach(r => {
      r.filter(_.asInstanceOf[Event].keyMap.nonEmpty).foreach(row => {
        val cells = new mutable.HashMap[String, Any]
        row.asInstanceOf[Event].keyMap.map(col => {
          cells(col._1) = PrimitiveObjInspectorFactory.stringObjConversion(col._2, outputFormat(col._1))
        })
        table.insert(MongoDBObject(cells.toList))
      })
    })

  }
}
