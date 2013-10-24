package thunderainproject.thunderain.example.cloudstone

import thunderainproject.thunderain.framework.output.{WritableObjectConvertFactory, PrimitiveObjInspectorFactory, AbstractEventOutput}
import thunderainproject.thunderain.framework.Event
import org.apache.spark.streaming.DStream
import tachyon.client.{WriteType, TachyonFS}
import java.nio.{ByteOrder, ByteBuffer}
import shark.SharkEnv
import scala.collection.mutable
import shark.memstore2.{TablePartition, TablePartitionStats}
import org.apache.spark.storage.StorageLevel
import shark.execution.serialization.JavaSerializer
import org.apache.spark.rdd.RDD
import shark.memstore2.column.ColumnBuilder
import org.apache.spark.Logging


/**
 * Output the data structure to tachyon table
 * 1. get the format automatically from the meta info from shark
 * 2. ser/der the column accordingly onto the tachyon
 * 3. persist the meta-data onto the tachyon for offline accessing
 * 4. union the older data with newly updates(also deleting obsolete data based on the DATA_TTL)
 *
 */
class TachyonRDDOutput extends AbstractEventOutput with Logging{
  //TODO to load the outputFormat automatically from shark/hive tableInfo
  @transient lazy val outputFormat = mutable.LinkedHashMap (
    ("h_host_ip", "String"),
    ("h_data_type", "String"),
    ("h_data_source", "String") ,
    ("h_user", "String") ,
    ("h_tags", "String") ,
    ("h_time", "Long"),
    ("b_message", "String"),
    ("b_log_level", "String"),
    ("b_trace", "String"),
    ("b_module_name", "String"),
    ("b_others", "String"),
    ("b_pid", "String"),
    ("b_tid", "String"),
    ("b_thread_name", "String"),
    ("b_source_file", "String"),
    ("b_line_number", "String")
    )


  val tachyonURL = System.getenv("TACHYON_MASTER")
  val tachyonWarehousePath = System.getenv("TACHYON_WAREHOUSE_PATH")

  @transient val tachyonClient = TachyonFS.get(tachyonURL)
  @transient lazy val tachyonClientOnSlave = TachyonFS.get(tachyonURL)
  lazy val tablePath = tachyonWarehousePath + "/" + outputName
  @transient lazy val table = tachyonClientOnSlave.getRawTable(tablePath)
  var rawTableId = -1;
  val timeColumnIndex = {
    outputFormat.keys.zipWithIndex.toMap.apply("h_time")
  }

  if (!tachyonClient.exist(tachyonWarehousePath)) {
    tachyonClient.mkdir(tachyonWarehousePath)
  }

  val cleanBefore = if (System.getenv("DATA_CLEAN_TTL") == null) {
    -1
  } else {
    System.getenv("DATA_CLEAN_TTL").toInt
  }

  val COLUMN_SIZE = 1000
  private var checkpointTm = System.currentTimeMillis() / 1000
  val CHECKPOINT_INTERVAL = 600

  /**
   * set the output name, derivatives can use it to set output name.
   */
  override def setOutputName(name: String) {
    val tblName = name + "_tachyon"
    super.setOutputName(tblName)
    super.setOutputDataFormat(outputFormat.values.toArray)
  }

  /**
   * set the output data format, derivatives can use it to set formats.
   */
  override def setOutputDataFormat(formats: Array[String]) = ()

  private def createMetaCol(rowNum: Long, part: Int) {
    val metaCol = table.getRawColumn(0)
    var file = metaCol.getPartition(part)
    if (file != null) {
      tachyonClientOnSlave.delete(file.getPath(), true)
    }
    metaCol.createPartition(part)
    file = metaCol.getPartition(part)

    val os = metaCol.getPartition(part).getOutStream(WriteType.TRY_CACHE)
    val buf = ByteBuffer.allocate(8)
    buf.order(ByteOrder.nativeOrder()).putLong(rowNum).flip()
    os.write(buf.array)
    os.close()
  }

  override def preprocessOutput(stream: DStream[_]): DStream[_] = {
    //println("outputFormat: " + outputFormat.mkString("[","],[","]"))
    if (tachyonClient.exist(tablePath)) {
      //TODO must we delete the data before hand?
      tachyonClient.delete(tablePath, true)
    }
    rawTableId = tachyonClient.createRawTable(tablePath, formats.length + 1)

    tachyonClient.close()
    stream
  }

  override def output(stream: DStream[_]) {
    stream.foreach(r => {
      val statAccum = SharkEnv.sc.accumulableCollection(mutable.ArrayBuffer[(Int, TablePartitionStats)]())

      val tblRdd = if (cleanBefore == -1) {
        //println("buildTachyonRdd here")
        buildTachyonRdd(r, statAccum)
      } else {
        val rdd = SharkEnv.memoryMetadataManager.get(outputName) match {
          case None => buildTachyonRdd(r, statAccum)
          case Some(s) => zipTachyonRdd(s, r, statAccum)
        }
        val currTm = System.currentTimeMillis() / 1000
        if (currTm - checkpointTm >= CHECKPOINT_INTERVAL) {
          rdd.checkpoint()
          checkpointTm = currTm
        }
        rdd
      }

      //println("tblRdd partition#: " + tblRdd.partitions.length)
      //TODO if for Tachyon storage only, we can remove the following line
      tblRdd.persist(StorageLevel.MEMORY_ONLY)
      //empty action
      tblRdd.foreach(_ => Unit)  //to trigger spark job in order to evaluate it right now


      // put rdd and statAccum to cache manager
      SharkEnv.memoryMetadataManager.put(outputName, tblRdd)
      SharkEnv.memoryMetadataManager.putStats(outputName, statAccum.value.toMap)
      tachyonClientOnSlave.updateRawTableMetadata(rawTableId,
        ByteBuffer.wrap(JavaSerializer.serialize(statAccum.value.toMap)))


    })
  }

  private def buildTachyonRdd(rdd: RDD[_],
                              stat: org.apache.spark.Accumulable[mutable.ArrayBuffer[(Int, TablePartitionStats)], (Int, TablePartitionStats)])
  = {
    val newRdd = rdd.mapPartitionsWithIndex((index, iter) => {
      val objInspectors = formats.map(PrimitiveObjInspectorFactory.newPrimitiveObjInspector(_))
      val colBuilders = objInspectors.map(i => ColumnBuilder.create(i))
      colBuilders.foreach(c => c.initialize(COLUMN_SIZE))

      var numRows = 0;
      iter.foreach(row => {
        val keyMap = row.asInstanceOf[Event].keyMap
        if(keyMap("h_time").nonEmpty && !keyMap("h_time").equals("")) {
          //println("row: " + keyMap.mkString("[","],[","]"))
          //println("schema: " + outputFormat.mkString("[","],[","]"))
          outputFormat.zipWithIndex.foreach(r => {
            val obj = {
              //TODO to move it into parser or other transformation part
              if (r._1._1.equals("h_time")) keyMap(r._1._1).toLong.asInstanceOf[Object]
              else keyMap(r._1._1).asInstanceOf[Object]
            }
            //println("column: (" + r._2 + ")" + r._1._1 + "=" + obj)
            colBuilders(r._2).append(obj, objInspectors(r._2))

          })

          numRows += 1
        }
      })

//      // to update TablePartitionStats
//      val tblPartStats = new TablePartitionStats(colBuilders.map(_.stats), numRows)
//      stat += Tuple2(index, tblPartStats)

      val colBuffers = {
        if(numRows > 0) new Array[ByteBuffer](colBuilders.size)
        else Array[ByteBuffer]()
      }

      if(numRows > 0) {
        colBuilders.zipWithIndex.foreach(c => {
          val col = table.getRawColumn(c._2 + 1)
          var file = col.getPartition(index)
          if (file != null) {
            tachyonClientOnSlave.delete(file.getPath(), true)
          }
          col.createPartition(index)
          file = col.getPartition(index)

          val os = file.getOutStream(WriteType.TRY_CACHE)
          val _buffer = c._1.build
          colBuffers(c._2) = _buffer
          os.write(_buffer.array)
          os.close()
        })
        createMetaCol(numRows, index)
      }
      //Iterator(new TablePartition(numRows, colBuilders.map(_.build)))
      Iterator(new TablePartition(numRows, colBuffers))
    })

    newRdd
  }

  private def zipTachyonRdd(oldRdd: RDD[_], newRdd: RDD[_],
                            stat: org.apache.spark.Accumulable[mutable.ArrayBuffer[(Int, TablePartitionStats)], (Int, TablePartitionStats)]) = {
    //println("oldRDD partition#: " + oldRdd.partitions.length + "and newRDD partition#: " + newRdd.partitions.length)
    val zippedRdd = oldRdd.asInstanceOf[RDD[TablePartition]].zipPartitions(newRdd.asInstanceOf[RDD[Any]])(
      (i1: Iterator[TablePartition], i2: Iterator[Any]) => {
        val objInspectors = formats.toArray.map(PrimitiveObjInspectorFactory.newPrimitiveObjInspector(_))
        val colBuilders = objInspectors.map(i => ColumnBuilder.create(i))
        colBuilders.foreach(c => c.initialize(COLUMN_SIZE))

        var numRows = 0
        val currTm = System.currentTimeMillis() / 1000
        i1.foreach(t => {
          t.iterator.foreach(c => {
            val row = c.getFieldsAsList
            val tm = WritableObjectConvertFactory.writableObjectConvert(row.get(timeColumnIndex),
              formats(timeColumnIndex)).asInstanceOf[Long]
            //println("currTm: " + currTm + " recTm: " + tm)
            if (row.get(1) != null && row.get(2) != null && (currTm - tm) < cleanBefore) {
                for(j <- 0 to row.size() -1) {
                  colBuilders(j).append(
                    WritableObjectConvertFactory.writableObjectConvert(row.get(j),
                      formats(j)), objInspectors(j)
                  )
                }
              numRows += 1
            }
          })
        })

        i2.foreach(row => {
          val keyMap = row.asInstanceOf[Event].keyMap
          if(keyMap("h_time").nonEmpty && !keyMap("h_time").equals("")) {
            //println("row: " + keyMap.mkString("[","],[","]"))
            outputFormat.zipWithIndex.foreach(r =>{
              val obj = {
                //TODO to move it into parser or other transformation part
                if (r._1._1.equals("h_time")) keyMap(r._1._1).toLong.asInstanceOf[Object]
                else keyMap(r._1._1).asInstanceOf[Object]
              }
              //println("column: (" + r._2 + ")" + r._1._1 + "=" + obj)
              colBuilders(r._2).append(obj, objInspectors(r._2))

            })
            numRows += 1
          }
        })

//        val tblPartStats = new TablePartitionStats(colBuilders.map(_.stats), numRows)

        if(numRows > 0) Iterator(new TablePartition(numRows, colBuilders.map(_.build)))
        else Iterator(new TablePartition(0, Array[ByteBuffer]()))
      }).mapPartitionsWithIndex((index, iter) => {
      val partition = iter.next()
      if(partition.numRows > 0) {
        partition.toTachyon.zipWithIndex.foreach(r => {
          val col = table.getRawColumn(r._2)
          var file = col.getPartition(index)
          if (file != null) {
            tachyonClientOnSlave.delete(file.getPath(), true)
          }
          col.createPartition(index)
          file = col.getPartition(index)
          val os = file.getOutStream(WriteType.TRY_CACHE)
          os.write(r._1.array)
          os.close()

        })
      }
      Iterator(partition)
    })

    zippedRdd
  }

}
