/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package thunderainproject.thunderain.example.cloudstone

import org.apache.spark.Logging

import thunderainproject.thunderain.framework.parser.AbstractEventParser
import thunderainproject.thunderain.framework.Event
import scala.util.parsing.json._
import scala.Some

//TODO to move the parser out of the example package
class JSONParser extends AbstractEventParser with Logging{
  override def parseEvent(event: String, schema: Array[String]) = {
    val eventMap : Map[String, String] = {
      try {
        JSON.parseFull(event) match {
          case Some(kvs : Map[String, String]) => {
            logDebug("Parsed event: " + kvs.mkString("[", "][", "]"))
            kvs
          }
          case None => {
            throw new Exception("Nothing parsed from event: " + event)
          }
        }
      }
      catch {
        case _ =>  {
          logError("Failed to parse JSON object:"  + event)
          schema.zip(schema.map(s => "")).toMap
        }
      }
    }

    new Event(System.currentTimeMillis()/ 1000, eventMap)
  }

}

object JSONParser {
  //to test the JSON parser only, this main class won't be open to the end user
  def main(args: Array[String]) {
      val log = """{ "h_time" : "1381807235" , """ +
      """ "h_host_ip" : "10.1.0.56" , """ +
      """ "h_user" : "Grace" , """ +
      """ "h_data_type" : "tachyonlog" ,""" +
      """ "h_data_source" : "master-tachyon.log" ,""" +
      """ "h_tags" : "" ,""" +
      """ "test": { "k1": "v1", "k2" : "v2" }, """ +
      """ "b_message" : "2013-10-10 19:50:44,811 INFO  MASTER_LOGGER (MasterInfo.java:getClientFileInfo)""" +
      """ - getClientFileInfo(/user/Grace/core-site.xml) """ + "\\n\"}"

    println(log)
    val parser = new JSONParser

    val schemas = Array("time", "host_ip", "user", "data_type",
      "data_source", "tags", "event")
    val ev = parser.parseEvent(log, schemas)

    println(ev)
  }
}

