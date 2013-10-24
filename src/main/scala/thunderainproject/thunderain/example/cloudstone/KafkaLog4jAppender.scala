package thunderainproject.thunderain.example.cloudstone

import kafka.producer.{Producer, ProducerConfig, ProducerData}
import kafka.producer.async.MissingConfigException
import org.apache.log4j.spi.LoggingEvent
import org.apache.log4j.AppenderSkeleton
import org.apache.log4j.helpers.LogLog
import kafka.utils.Logging
import java.util.{Properties, Date}
import scala.collection.mutable

/**
 * A little modifications on KafkaLo4jAppender 0.7.2
 * 1. add one subappend function, which can be overridden by its child class
 * 2. applied KAFKA-524 patch file to solve the zookeeper deadlock issue
 */
class KafkaLog4jAppender extends AppenderSkeleton with Logging {
  var port:Int = 0
  var host:String = null
  var topic:String = null
  var serializerClass:String = null
  var zkConnect:String = null
  var zkConnectTimeout:String = null
  var zkSessionTimeout:String = null
  var brokerList:String = null

  private var producer: Producer[String, String] = null
  private var config: ProducerConfig = null
  @volatile private var initialized:Boolean = false

  def getTopic:String = topic
  def setTopic(topic: String) { this.topic = topic }


  def getZkConnect:String = zkConnect
  def setZkConnect(zkConnect: String) { this.zkConnect = zkConnect }

  def getZkConnectTimeout:String = zkConnectTimeout
  def defZkConnectTimeout(zkConnectTimeout: String) {
    this.zkConnectTimeout = zkConnectTimeout  }

  def getZkSessionTimeout:String = zkSessionTimeout
  def setZkConnectTimeout(zkConnectTimeout: String) {
    this.zkSessionTimeout = zkSessionTimeout  }

  def getBrokerList:String = brokerList
  def setBrokerList(brokerList: String) { this.brokerList = brokerList }

  def getSerializerClass:String = serializerClass
  def setSerializerClass(serializerClass:String) { this.serializerClass = serializerClass }


  override def activateOptions() {
    val connectDiagnostic : mutable.ListBuffer[String] = mutable.ListBuffer();
    // check for config parameter validity
    val props = new Properties()
    if( zkConnect == null) connectDiagnostic += "zkConnect"
    else {
      props.put("zk.connect", zkConnect)
      if(zkConnectTimeout != null)
        props.put("zk.connectiontimeout.ms", zkConnectTimeout)
      if(zkSessionTimeout != null)
        props.put("zk.sessiontimeout.ms", zkSessionTimeout)
    }

    if( brokerList == null) connectDiagnostic += "brokerList"
    else if( props.isEmpty) props.put("broker.list", brokerList)
    if(props.isEmpty )
      throw new MissingConfigException(
        connectDiagnostic mkString ("One of these connection properties must be specified: ", ", ", ".")
      )
    if(topic == null)
      throw new MissingConfigException("topic must be specified by the Kafka log4j appender")
    if(serializerClass == null) {
      serializerClass = "kafka.serializer.StringEncoder"
      LogLog.warn("Using default encoder - kafka.serializer.StringEncoder")
    }
    props.put("serializer.class", serializerClass)
    config = new ProducerConfig(props)
  }

  override def append(event: LoggingEvent)  {
    //AppenderSkeleton#append serialized via AppenderSkeletion#doAppend
    // so it is safe to do this
    if(!initialized) {
      producer = new Producer[String, String](config)
      LogLog.debug("Kafka producer connected to " + (if(config.zkConnect == null) config.brokerList else config.zkConnect))
      LogLog.debug("Logging for topic: " + topic)
      initialized = true
    }
    val message = subappend(event)
    LogLog.debug("[" + new Date(event.getTimeStamp).toString + "]" + message)
    val messageData : ProducerData[String, String] =
      new ProducerData[String, String](topic, message)
    producer.send(messageData);
  }

  def subappend(event: LoggingEvent): String  = {
    val message : String = if( this.layout == null) {
      event.getRenderedMessage
    }
    else this.layout.format(event)
    message
  }

  override def close() {
    if(!this.closed) {
      this.closed = true
      producer.close()
      if(initialized)
        producer.close()
    }
  }


  override def requiresLayout: Boolean = false
}