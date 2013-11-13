package thunderainproject.thunderain.example.cloudstone.operations

import scala.xml.Node

import org.apache.spark.streaming.DStream

import thunderainproject.thunderain.framework.Event
import thunderainproject.thunderain.framework.operator.{OperatorConfig, AbstractOperator}
import thunderainproject.thunderain.framework.output.AbstractEventOutput

class IdentityOperator extends AbstractOperator with OperatorConfig {
  class IdentityOperatorConfig (
                              val name: String,
                              val window: Option[Long],
                              val slide: Option[Long],
                              val partitionNum: Int,
                              val outputClzs: Array[String],
                              val outputArgs: Array[String]) extends Serializable

  override def parseConfig(conf: Node) {
    val nam = (conf \ "@name").text

    val propNames = Array("@window", "@slide")
    val props = propNames.map(p => {
      val node = conf \ "property" \ p
      if (node.length == 0) {
        None
      } else {
        Some(node.text)
      }
    })

    val partitionNum = (conf \ "partitions").text

    val outputProps = Array("@class", "@args").map( p => {
      (conf \ "outputs" \ "output" ).map(output => {
        val node = output \ p
        if(node.length == 0) ""
        else node.text
      })
    })

    val outputs = outputProps(0).toArray
    val argses = outputProps(1).toArray


    config = new IdentityOperatorConfig(
      nam,
      props(0) map { s => s.toLong },
      props(1) map { s => s.toLong },
      partitionNum.toInt,
      outputs,
      argses)



    outputClzs = new Array[AbstractEventOutput](outputs.size)
    for(i <- 0 until outputs.size) {
      outputClzs(i) = try {
        Class.forName(config.outputClzs(i)).newInstance().asInstanceOf[AbstractEventOutput]
      } catch {
        case e: Exception => throw new Exception("class " + config.outputClzs(i) + " new instance failed")
      }
      outputClzs(i).setOutputName(config.name)
      outputClzs(i).setArgs(config.outputArgs(i))
    }
  }

  protected var config: IdentityOperatorConfig = _
  protected var outputClzs: Array[AbstractEventOutput] = _

  override def process(stream: DStream[Event]) {
    val windowedStream = windowStream(stream, (config.window, config.slide))
    //set the partition number
    val resultStream = windowedStream
      .transform(r => r.coalesce(config.partitionNum, true))

    outputClzs.map(clz => clz.output(clz.preprocessOutput(resultStream)))
  }
}
