package thunderainproject.thunderain.example.cloudstone.agents

import org.apache.log4j.spi.LoggingEvent

class CloudstoneKafkaLog4jAppender extends KafkaLog4jAppender {
  override def subAppend(event: LoggingEvent) = {
    topic + "|||" + super.subAppend(event)
  }

  //requires layout
  override def requiresLayout: Boolean = true
}

