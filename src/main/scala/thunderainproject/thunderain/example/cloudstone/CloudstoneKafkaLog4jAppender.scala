package thunderainproject.thunderain.example.cloudstone

import org.apache.log4j.spi.LoggingEvent
import org.apache.log4j.helpers.LogLog

import java.net.InetAddress
import java.lang.management.ManagementFactory

import sun.jvmstat.monitor._

import scala.util.parsing.json._
import scala.collection.mutable
import scala.reflect.BeanProperty


class CloudstoneKafkaLog4jAppender extends KafkaLog4jAppender {
  // to configure tags
  @BeanProperty var tags: String = ""
  lazy val pid: String = {
    val name = ManagementFactory.getRuntimeMXBean().getName()
    val pidPattern = """(\d+)@.*""".r
    name match {
      case pidPattern(s) => s
      case _ => {
        LogLog.error("Failed to parsing the PID number for current java process")
        ""
      }
    }
  }

  // to configure data_source
  @BeanProperty var dataSource: String = {
    try {
      // combine java cmd line with its arguments as the default source data
      // 1. to get the pid of the system
      // 2. makeup a vmid
      val vmidString = "//" + pid + "?mode=r"
      // 3. get main class with its arguments
      val hostId = new HostIdentifier("//localhost")
      val monitoredHost = MonitoredHost.getMonitoredHost(hostId)
      val id = new VmIdentifier(vmidString)
      val vm = monitoredHost.getMonitoredVm(id, 0)
      val mainClass = MonitoredVmUtil.mainClass(vm,false)
      val mainArgs = MonitoredVmUtil.mainArgs(vm)
      // 4. get jvm opts including flags and arguments
      val jvmArgs = MonitoredVmUtil.jvmArgs(vm)
      val jvmFlags = MonitoredVmUtil.jvmFlags(vm)

      pid + " " + jvmArgs + " " + jvmFlags + mainClass + " " + mainArgs
    }
    catch {
      // if no right tool.jar available, only returns PID
      case _ => pid
    }

  }
  // to configure data_type
  @BeanProperty var dataType: String = null
  // emit time
  def time = {
    System.currentTimeMillis() / 1000
  }
  // to configure the user
  @BeanProperty var user: String = {
    System.getProperty("user.name")
  }
  // to configure the host ip
  @BeanProperty var hostIP: String = {
    InetAddress.getLocalHost().getHostAddress()
  }

  private var messageHeader = ""
  private var keyMap = new mutable.HashMap[String, Any]()

  override def subappend(event: LoggingEvent) = {
    //make the trace information if the layout ignores it
    var trace = ""
    if(layout != null && layout.ignoresThrowable()) {
      val s = event.getThrowableStrRep()
      if (s != null ) {
        for (i <- s) {
          trace += i  + "<br>"//Layout.LINE_SEP
        }
      }
    }

    keyMap("h_time") = time.toString
    keyMap("b_log_level") = event.getLevel.toString
    keyMap("b_trace") = trace
    keyMap("b_module_name") = event.getLocationInformation.fullInfo
    keyMap("b_thread_name") = event.getThreadName
    keyMap("b_source_file") = event.getLocationInformation.getFileName
    keyMap("b_line_number") = event.getLocationInformation.getLineNumber
    keyMap("b_others") = ""
    keyMap("b_pid") = pid
    keyMap("b_tid") = Thread.currentThread().getId.toString
    keyMap("b_message") = super.subappend(event).stripLineEnd
    val message = JSONObject(keyMap.toMap).toString()

    LogLog.debug("new message in JSON: " + message)

    // append the header of the stream-out data
    topic + "|||" + "{" + message + "}"
  }

  override def activateOptions() {
    super.activateOptions()
    if(dataType == null) setDataType(getTopic)
    keyMap("h_host_ip") = hostIP
    keyMap("h_user") = user
    keyMap("h_tags") = tags
    keyMap("h_data_source")= dataSource
    keyMap("h_data_type") = dataType
  }

  override def requiresLayout: Boolean = true
}

