package thunderainproject.thunderain.example.cloudstone

import _root_.java.lang.System
import thunderainproject.thunderain.framework.Event
import org.apache.spark.streaming.{DStream, TestSuiteBase}
import tachyon.LocalTachyonCluster
import tachyon.client.TachyonFS

class TachyonRDDOutputTestSuite extends TestSuiteBase{
  var mLocalTachyonCluster: LocalTachyonCluster = _
  var mTfs: TachyonFS  = _

  before {
    System.setProperty("tachyon.user.quota.unit.bytes", "1000")
    mLocalTachyonCluster = new LocalTachyonCluster(10000)
    mLocalTachyonCluster.start()
    mTfs = mLocalTachyonCluster.getClient()
  }

  after {
    mLocalTachyonCluster.stop()
    System.clearProperty("tachyon.user.quota.unit.bytes")
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")
  }

  test("create tachyon warehouse"){

  }

  test("append new records") {

  }

  test("read old records") {

  }

  test("Persistent to filesystem") {

  }

  test("build table partition") {

  }

}
