package thunderainproject.thunderain.example.cloudstone

import org.scalatest.{FunSuite}
import com.mongodb.casbah.Imports._
import scala.collection.mutable
import scala.collection.immutable
import thunderainproject.thunderain.framework.Event
import thunderainproject.thunderain.framework.output.PrimitiveObjInspectorFactory


class MongoDBOutputSuite extends FunSuite{

  def fixture = {
    new {
      val mongoURL = MongoClientURI("mongodb://localhost:27017")
      val mongoDB = "test"

      lazy val mongoDBClientOnSlave = MongoClient(mongoURL).apply(mongoDB)
      val table = mongoDBClientOnSlave("test")
    }
  }


  test("collection maker") {
    val f = fixture
    lazy val outputFormat = mutable.LinkedHashMap (
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


    val row = new Event(System.currentTimeMillis()/ 1000, Array(("h_host_ip", "10.0.0.1"),
      ("h_data_type", "test"),
      ("h_data_source", "scalatest") ,
      ("h_user", "Grace") ,
      ("h_tags", "") ,
      ("h_time", "1234567890"),
      ("b_message", "this is only for test")
    ).toMap)
    val cells = new mutable.HashMap[String, Any]
    row.asInstanceOf[Event].keyMap.map(col => {
      cells(col._1) = PrimitiveObjInspectorFactory.stringObjConversion(col._2, outputFormat(col._1))
      assert(cells(col._1).getClass.getSimpleName == outputFormat(col._1),
        "The value is type of " + cells(col._1).getClass.getSimpleName)
    })

    f.table.drop()
    assert(f.table.count() === 0, "Drop failed. test Collection has " + f.table.count() +" documents")
    f.table.insert(MongoDBObject(cells.toList))
    assert(f.table.count() === 1, "Insertion failed. test Collection has " + f.table.count() +" documents")

    f.table.findOne() match {
      case Some(record) => {
        record.filter(_._1 != "_id").foreach(r => {
          assert(cells(r._1) === r._2, "cells(" + r._1 + ") should be " + r._2)
        })
      }
      case None => assert(false, "Cannot find that inserted record")
    }
  }

  test("mongoDB insertion") {
    val f = fixture
    f.table.drop()
    assert(f.table.count() === 0, "Drop failed. test Collection has " + f.table.count() +" documents")
    val cells = new mutable.HashMap[String, Any]
    cells("x")=1
    cells("y")="2"
    cells("z")=3
    f.table.insert(MongoDBObject(cells.toList))
    assert(f.table.count() === 1, "Insertion failed. test Collection has " + f.table.count() +" documents")

    f.table.findOne() match {
      case Some(record) => {
        record.filter(_._1 != "_id").foreach(r => {
          assert(cells(r._1) === r._2)
        })
      }
      case None => assert(false, "Cannot find that inserted record")
    }
  }

}
