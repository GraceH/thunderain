package thunderainproject.thunderain.framework

import org.apache.spark.streaming.scheduler.{StreamingListenerBatchCompleted, BatchInfo, StreamingListenerBatchStarted, StreamingListener}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import scala.collection.mutable.{Set,HashSet,SynchronizedSet}

trait BatchTimeoutHandler {
  def handleTimeoutEvent
}

class JobGroupCancelHandler(sc: SparkContext, groupId: String) extends BatchTimeoutHandler {
  def handleTimeoutEvent: Unit = {
    sc.cancelJobGroup(groupId)
  }
}

class TimerThread(timeoutThresholdInMS: Long) extends Thread {
  @volatile var startTime: Long = _
  val handlers: Set[BatchTimeoutHandler]
    = new HashSet[BatchTimeoutHandler] with SynchronizedSet[BatchTimeoutHandler]


  override def run() {
    while (true) {
      val elapsedTime = System.currentTimeMillis() - startTime
      if(elapsedTime > timeoutThresholdInMS && startTime > 0){
        handlers.map(_.handleTimeoutEvent) //Todo parallel
        startTime = 0
      }
      Thread.sleep(timeoutThresholdInMS / 5)
    }
  }

  def setStartTime(initTime: Long) {
    startTime = initTime
  }

  override def start() {
    super.start()
  }

  def registerHandler(handler: BatchTimeoutHandler) {
    handlers += handler
  }

  def removeHandler(handler: BatchTimeoutHandler) {
    handlers -= handler
  }
}

class Listener(ssc: StreamingContext, batchDurationInSecond: Int) extends StreamingListener {

  val timer : TimerThread = new TimerThread(batchDurationInSecond * 2000)
  timer.start()

  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted) {
    timer.setStartTime(batchStarted.batchInfo.processingStartTime.get)
  }

  override def onBatchCompleted(batchCompleted : StreamingListenerBatchCompleted) {

  }

  def addHandler(handler: BatchTimeoutHandler) {
    timer.registerHandler(handler)
  }

  def dropHandler(handler: BatchTimeoutHandler) {
    timer.removeHandler(handler)
  }
}



