package org.apache.spark.thunderainproject.thunderain.example.cloudstone.output

import org.apache.spark.scheduler._
import thunderainproject.thunderain.example.cloudstone.output.TachyonRDDOutput
import org.apache.spark.Success
import scala.collection.mutable.{Set,HashSet}

class TachyonRDDOutputListener(tRDD: TachyonRDDOutput) extends SparkListener{
  var stageIds: Set[Int] = new HashSet[Int]()

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    if(stageIds.apply(taskEnd.task.stageId)){
      taskEnd.reason match {
        case Success => {}
        case _ => {
          tRDD.Rollback(4, tRDD.tableKey)
        }
      }
    }
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) {
    if(stageSubmitted.stage.name.contains("TachyonRDDOutput.scala") ) {
      stageIds -= stageSubmitted.stage.stageId
    }
  }

  override def onJobStart(jobStart: SparkListenerJobStart) {
    if("TachyonOutputs" == jobStart.job.properties.get("spark.jobGroup.id")) {
      jobStart.stageIds.foreach(stageIds += _)
    }
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd) {
    if("TachyonOutputs" == jobEnd.job.properties.get("spark.jobGroup.id")) {
        jobEnd.jobResult match {
        case JobSucceeded => {
          tRDD.commit(4, tRDD.tableKey)
        }
        case JobFailed(e: Exception, _) => {
          tRDD.Rollback(4, tRDD.tableKey)
        }
      }
    }
  }
}
