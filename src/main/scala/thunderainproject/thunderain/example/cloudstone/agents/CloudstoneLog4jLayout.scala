package thunderainproject.thunderain.example.cloudstone.agents

import org.apache.log4j.{Layout, PatternLayout}
import org.apache.log4j.spi.LoggingEvent
import java.lang.management.ManagementFactory
import org.apache.log4j.helpers.LogLog
import scala.reflect.BeanProperty
import sun.jvmstat.monitor.{MonitoredVmUtil, VmIdentifier, MonitoredHost, HostIdentifier}
import java.net.InetAddress
import scala.collection.mutable
import scala.util.parsing.json.JSONObject

trait CloudstoneLog4jLayout extends Layout{

  private val LINE_SEP = "<br>"
  // to configure tags
  var tags: String = ""

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

      pid + " " + jvmArgs + " " + jvmFlags + " " + mainClass + " " + mainArgs
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
    System.currentTimeMillis()
  }
  // to configure the user
  @BeanProperty var user: String = {
    System.getProperty("user.name")
  }
  // to configure the host ip
  @BeanProperty var hostIP: String = {
    InetAddress.getLocalHost.getHostAddress
  }

  private var keyMap = new mutable.HashMap[String, Any]()

  abstract override def	activateOptions() = {
    super.activateOptions()
    keyMap("h_host_ip") = hostIP
    keyMap("h_user") = user
    keyMap("h_tags") = tags
    keyMap("h_data_source")= dataSource
    keyMap("h_data_type") = dataType
  }

  abstract override def format(event: LoggingEvent): String = {
    //make the trace information if the layout ignores it
    var trace = ""
    if(this.ignoresThrowable()) {
      val s = event.getThrowableStrRep
      if (s != null ) {
        for (i <- s) {
          trace += i  + LINE_SEP//Layout.LINE_SEP
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
    keyMap("b_tid") = Thread.currentThread.getId.toString
    keyMap("b_message") = super.format(event).replaceAll(Layout.LINE_SEP, LINE_SEP).stripLineEnd
    val message = JSONObject(keyMap.toMap).toString()

    LogLog.debug("New message in JSON: " + message)

    // append the header of the stream-out data
    // which is unique for the thunderain project only
    message + Layout.LINE_SEP
  }
}

class CloudstoneLog4jPatternLayout extends PatternLayout with CloudstoneLog4jLayout
//class CloudstoneLog4jDateLayout extends org.apache.log4j.helpers.DateLayout with CloudstoneLog4jLayout
class CloudstoneLog4jEnhancedPatternLayout extends org.apache.log4j.EnhancedPatternLayout with CloudstoneLog4jLayout
class CloudstoneLog4jSimpleLayout extends org.apache.log4j.SimpleLayout with CloudstoneLog4jLayout
class CloudstoneLog4jXMLLayout extends org.apache.log4j.xml.XMLLayout with CloudstoneLog4jLayout