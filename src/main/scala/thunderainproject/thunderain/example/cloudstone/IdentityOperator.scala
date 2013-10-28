package thunderainproject.thunderain.example.cloudstone

import scala.xml.Node

import org.apache.spark.streaming.DStream

import thunderainproject.thunderain.framework.Event
import thunderainproject.thunderain.framework.operator.{OperatorConfig, AbstractOperator}
import thunderainproject.thunderain.framework.output.AbstractEventOutput

/**
 * Simply store the parsed data into its corresponding column
 */
class IdentityOperator extends AbstractOperator with OperatorConfig {
  class IdentityOperatorConfig (
                              val name: String,
                              val window: Option[Long],
                              val slide: Option[Long],
                              val partitionNum: Int,
                              val outputClsName: String) extends Serializable

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
    val output = (conf \ "output").text
    val args = (conf \ "outputargs").text

    config = new IdentityOperatorConfig(
      nam,
      props(0) map { s => s.toLong },
      props(1) map { s => s.toLong },
      partitionNum.toInt,
      output)

    outputCls = try {
      Class.forName(config.outputClsName).newInstance().asInstanceOf[AbstractEventOutput]
    } catch {
      case e: Exception => throw new Exception("class " + config.outputClsName + " new instance failed")
    }
    outputCls.setOutputName(config.name)
    outputCls.setArgs(args)
  }

  protected var config: IdentityOperatorConfig = _
  protected var outputCls: AbstractEventOutput = _

  override def process(stream: DStream[Event]) {
    val windowedStream = windowStream(stream, (config.window, config.slide))
    //set the partition number
    val resultStream = windowedStream
      .transform(r => r.coalesce(config.partitionNum, true))

    outputCls.output(outputCls.preprocessOutput(resultStream))
  }
}
