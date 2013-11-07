package thunderainproject.thunderain.example.cloudstone

import thunderainproject.thunderain.framework.Event
import org.apache.spark.streaming.{DStream, TestSuiteBase}
import tachyon.LocalTachyonCluster
import tachyon.client.TachyonFS
import shark.memstore2.TablePartition
import org.scalatest.BeforeAndAfterAll
import shark.SharkEnv

import java.nio.{ByteOrder, ByteBuffer}
import java.io.IOException
import scala.collection.immutable.HashMap
import thunderainproject.thunderain.example.cloudstone.output.TachyonRDDOutput


class CloudstoneOperationTestSuite extends TestSuiteBase with BeforeAndAfterAll{

  var mLocalTachyonCluster: LocalTachyonCluster = _
  var mTfs: TachyonFS  = _
  val tRddOutput = new TachyonRDDOutput
  val dataTTL = System.getenv("DATA_CLEAN_TTL")

  // create the test data
  def fixture = {
    new {
      lazy val currTm = System.currentTimeMillis() / 1000 - 4
      lazy val clean_ttl = {
        if(dataTTL == null) 0
        else dataTTL.toLong
      }
      lazy val testInputData = Seq(
        Seq(new Event(currTm, Array(
          ("h_time", (currTm).toString),
          ("b_message", "1-1")
        ).toMap),
          new Event(currTm, Array(
            ("h_time", (currTm).toString),
            ("b_message", "1-2")
          ).toMap),
          new Event(currTm, Array(
            ("h_time", (currTm).toString),
            ("b_message", "1-3")
          ).toMap)),
        Seq(new Event(currTm+1, Array(
          ("h_time", (currTm+1).toString),
          ("b_message", "2-1")
        ).toMap),
          new Event(currTm+1, Array(
            ("h_time", (currTm+1).toString),
            ("b_message", "2-2")
          ).toMap),
          new Event(currTm+1, Array(
            ("h_time", (currTm+1+clean_ttl).toString),
            ("b_message", "2-3")
          ).toMap)),
        Seq(new Event(currTm+2, Array(
          ("h_time", (currTm+2).toString),
          ("b_message", "3-1")
        ).toMap),
          new Event(currTm+2, Array(
            ("h_time", (currTm+2+clean_ttl).toString),
            ("b_message", "3-2")
          ).toMap),
          new Event(currTm+2, Array(
            ("h_time", (currTm+2+clean_ttl).toString),
            ("b_message", "3-3")
          ).toMap)),
        Seq(new Event(currTm+3, Array(
          ("h_time", (currTm+3+clean_ttl).toString),
          ("b_message", "4-1")
        ).toMap),
          new Event(currTm+3, Array(
            ("h_time", (currTm+3+clean_ttl).toString),
            ("b_message", "4-2")
          ).toMap),
          new Event(currTm+3, new HashMap()
            //("h_time", (currTm+3+dataTTL).toString),
            //("b_message", "4-3")
           //).toMap
          ))
      )

    }
  }

  override def beforeAll() {
    super.beforeAll()
    System.setProperty("tachyon.user.quota.unit.bytes", "1000")

    // prepare following env variables before execute the following test!
    //    "TACHYON_MASTER=localhost:18998"
    //    "TACHYON_WAREHOUSE_PATH=/user/tachyon"
    //    "DATA_CLEAN_TTL=10"

    mLocalTachyonCluster = new LocalTachyonCluster(10000)
    mLocalTachyonCluster.start()
    mTfs = mLocalTachyonCluster.getClient()

    tRddOutput.setOutputName("test_view")
    tRddOutput.setOutputFormat(Array[String]("h_time", "b_message"), Array[String]("Bigint", "String"))
    tRddOutput.setArgs("h_time")


  }

  override def afterAll() {
    super.afterAll()
    mLocalTachyonCluster.stop()
    System.clearProperty("tachyon.user.quota.unit.bytes")
  }

  before {
    System.setProperty("spark.streaming.clock", "org.apache.spark.streaming.util.ManualClock")
  }

  after {
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")
  }

  test("obtain tachyonWriter"){
    val f = fixture
    val inputData = f.testInputData
    val operator = (stream: DStream[Event]) => {
      tRddOutput.preprocessOutput(stream).asInstanceOf[DStream[Event]]
    }
    testOperation(inputData, operator, inputData, true)
    assert(tRddOutput.tachyonWriter != null, "Failed to initialize the tachyonWriter")
    assert(mTfs.exist(System.getenv("TACHYON_WAREHOUSE_PATH")), "No warehouse created")
  }

  test("identity operation") {
    val f = fixture
    val inputData = f.testInputData
    val identityOperator = (stream: DStream[Event]) =>  {
      stream.transform(r => r.coalesce(2, true))
    }

    testOperation(inputData, identityOperator, inputData, true)
  }


  test("write new TablePartition onto Tachyon") {
    // must run the test of "obtain tachyonWriter" before hand
    val f = fixture
    val inputData = f.testInputData
    val outputData: Array[TablePartition] = new Array[TablePartition](4)
    inputData.zipWithIndex.foreach{case(seq: Seq[Event], index: Int) => {
      val builder = tRddOutput.createPartitionBuilder()
      seq.foreach( event => {
        tRddOutput.addRow(builder, event, tRddOutput.createEventStructObjectInspector)
      })
      outputData(index) = builder.build
    }}

    tRddOutput.tachyonWriter.createTable(ByteBuffer.allocate(0))

    outputData.zipWithIndex.foreach{case(partition: TablePartition, partitionIndex: Int) => {
      tRddOutput.writeColumnPartition(partition, partitionIndex)
    }}

    assert(tRddOutput.tachyonWriter != null, "Failed to initialize the tachyonWriter")
    assert(mTfs.exist(System.getenv("TACHYON_WAREHOUSE_PATH")), "No warehouse created")

    val tablepath = System.getenv("TACHYON_WAREHOUSE_PATH")+ "/test_view_tachyon"
    assert(mTfs.exist(tablepath), "raw table: " + "test_view_tachyon"+  " doesn't exist")
    for (i <- 0 to 2) {
      assert(mTfs.exist(tablepath + "/COL_" + i) , "column_" + i +" doesn't exist")
      for(j <- 0 to 2) {
        assert(mTfs.exist(tablepath + "/COL_" + i +"/" + j) ,
          "column_" + i + " partition_" + j+" doesn't exist")
      }
    }

    //SharkEnv.tachyonUtil.dropTable("test_view_tachyon")

  }

  ignore("read tachyon records") {
    //TODO

  }


  test("overwrite the TablePartition onto Tachyon") {
    //this test must run after the "write new TablePartition onto Tachyon" test
    // must run the test of "obtain tachyonWriter" before hand
    val f = fixture
    val inputData = f.testInputData
    val outputData: Array[TablePartition] = new Array[TablePartition](4)
    inputData.zipWithIndex.foreach{case(seq: Seq[Event], index: Int) => {
      val builder = tRddOutput.createPartitionBuilder()
      seq.foreach( event => {
        tRddOutput.addRow(builder, event, tRddOutput.createEventStructObjectInspector)
      })
      outputData(index) = builder.build
    }}

    assert(tRddOutput.tachyonWriter != null, "Failed to initialize the tachyonWriter")
    assert(mTfs.exist(System.getenv("TACHYON_WAREHOUSE_PATH")), "No warehouse created")

    val tablepath = System.getenv("TACHYON_WAREHOUSE_PATH")+ "/test_view_tachyon"
    assert(mTfs.exist(tablepath), "raw table: " + "test_view_tachyon"+  " doesn't exist")
    for (i <- 0 to 2) {
      assert(mTfs.exist(tablepath + "/COL_" + i) , "column_" + i +" doesn't exist")
      for(j <- 0 to 2) {
        assert(mTfs.exist(tablepath + "/COL_" + i +"/" + j) ,
          "column_" + i + " partition_" + j+" doesn't exist")
      }
    }

    outputData.zipWithIndex.foreach{case(partition: TablePartition, partitionIndex: Int) => {
      //expect some file already exists message here
      tRddOutput.writeColumnPartition(partition, partitionIndex)
    }}

    assert(tRddOutput.tachyonWriter != null, "Failed to initialize the tachyonWriter")
    assert(mTfs.exist(System.getenv("TACHYON_WAREHOUSE_PATH")), "No warehouse created")

    assert(mTfs.exist(tablepath), "raw table: " + "test_view_tachyon"+  " doesn't exist")
    for (i <- 0 to 2) {
      assert(mTfs.exist(tablepath + "/COL_" + i) , "column_" + i +" doesn't exist")
      for(j <- 0 to 2) {
        assert(mTfs.exist(tablepath + "/COL_" + i +"/" + j) ,
          "column_" + i + " partition_" + j+" doesn't exist")
      }
    }


  }

}
