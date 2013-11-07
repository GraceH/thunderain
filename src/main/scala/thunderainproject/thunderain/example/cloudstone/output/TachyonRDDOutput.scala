package thunderainproject.thunderain.example.cloudstone.output

import thunderainproject.thunderain.framework.output.{AbstractEventOutput,PrimitiveObjInspectorFactory}

import org.apache.spark.rdd.RDD
import org.apache.spark.Logging
import org.apache.spark.Accumulable
import org.apache.spark.streaming.DStream


import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspectorUtils, ObjectInspector, StructField, StructObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo

import shark.memstore2.ColumnarStructObjectInspector.IDStructField
import shark.memstore2.{ColumnarStructObjectInspector, TablePartitionBuilder, TablePartition, TablePartitionStats}
import shark.execution.serialization.JavaSerializer
import shark.tachyon.TachyonTableWriter
import shark.{SharkEnvSlave, SharkEnv}

import scala._
import scala.collection.mutable
import scala.collection.JavaConversions._

import java.util.{List => JList, ArrayList => JArrayList}
import java.nio.ByteBuffer

import tachyon.client.WriteType


/**
 * Output the data structure to tachyon table
 * TODO 1. get the format automatically from the meta info from shark
 * 2. ser/der the column accordingly onto the tachyon
 * 3. persist the meta-data(including the states for PartitionPrune) onto the tachyon for offline accessing
 * 4. merge the older data with newly updates(deleting obsolete data based on the DATA_TTL)
 *
 *
 */
class TachyonRDDOutput extends AbstractEventOutput with Logging{
  initLogging()
  //TODO to load the outputFormat automatically from shark/hive tableInfo
  var fieldNames = Array[String]("h_host_ip", "h_data_type", "h_data_source", "h_user", "h_tags", "h_time",
                                     "b_message", "b_log_level", "b_trace", "b_module_name", "b_others",
                                     "b_pid","b_tid", "b_thread_name", "b_source_file", "b_line_number")

  var tachyonWriter: TachyonTableWriter = _
  var timeColumnIndex: Int = _

  val cleanBefore = if (System.getenv("DATA_CLEAN_TTL") == null) {
    -1
  } else {
    System.getenv("DATA_CLEAN_TTL").toInt
  }

  val COLUMN_SIZE = 1000
//  private var checkpointTm = System.currentTimeMillis() / 1000
//  val CHECKPOINT_INTERVAL = 600

  /**
   * Set and parsing the output arguments accordingly
   * @param args
   */
  override def setArgs(args: String): Unit = {
    val timestampFieldName = args.split(" ").apply(0)
    timeColumnIndex = fieldNames.zipWithIndex.toMap.apply(timestampFieldName)
  }

  /**
   * print out the usage for TachyonRddOutput
   */
  override def help(): Unit = {
    println(this.getClass.getSimpleName + " timestampFieldName")
  }

  override def setOutputName(name: String) {
    val tblName = name + "_tachyon"
    super.setOutputName(tblName)
    super.setOutputDataFormat(Array[String]("String","String","String", "String", "String", "Bigint",
                                                "String", "String", "String", "String", "String",
                                                "String", "String", "String", "String", "String"))
  }

  protected[cloudstone] def setOutputFormat(fieldNames: Array[String], fieldFormats: Array[String]) {
    this.fieldNames = fieldNames
    super.setOutputDataFormat(fieldFormats)
  }

  override def setOutputDataFormat(formats: Array[String]) = ()

  override def preprocessOutput(stream: DStream[_]): DStream[_] = {
    logDebug("obtain the tachyonWrite in preprocessOutput")
    //obtain the tachyonWriter from the shark util
    tachyonWriter = SharkEnv.tachyonUtil.createTableWriter(outputName, formats.length + 1)
    //no transformation for the input stream here
    stream
  }

  override def output(stream: DStream[_]) {
    stream.foreach(r => {
      val statAccum =
        stream.context.sparkContext.accumulableCollection(mutable.ArrayBuffer[(Int, TablePartitionStats)]())

      val tblRdd = if (cleanBefore == -1) {
        //overwrite the table with new input RDD data
        buildTachyonRdd(r, statAccum)
      } else {
        val oldRdd = readFromTachyon(outputName)
        try {zipTachyonRdd(oldRdd, r, statAccum)}
        catch{case _ => buildTachyonRdd(r, statAccum)}
        //checkpoint is not necessary for tachyon output
//        val currTm = System.currentTimeMillis() / 1000
//        if (currTm - checkpointTm >= CHECKPOINT_INTERVAL) {
//          rdd.checkpoint()
//          checkpointTm = currTm
//        }
      }

      //dummy output stream
      tblRdd.foreach(_ => Unit)  //to trigger spark job in order to evaluate it immediately

      // put rdd and statAccum to cache manager
      if (tachyonWriter != null && statAccum != null && SharkEnv.tachyonUtil.tableExists(outputName)) {
        //persist the stats onto tachyon file system, otherwise Shark cannot read those data from tachyon later
        tachyonWriter.updateMetadata(ByteBuffer.wrap(JavaSerializer.serialize(statAccum.value.toMap)))
      }
    })
  }

  protected[cloudstone] def buildTachyonRdd(rdd: RDD[_],
    stat: Accumulable[mutable.ArrayBuffer[(Int, TablePartitionStats)], (Int, TablePartitionStats)]): RDD[TablePartition] = {
    logDebug("To build tachyon rdd")
    val inputRdd = prepareTablePartitionRDD(rdd, stat)
    writeTablePartitionRDDToTachyon(inputRdd)
  }



  protected[cloudstone] def zipTachyonRdd(oldRdd: RDD[_], newRdd: RDD[_],
    stat: Accumulable[mutable.ArrayBuffer[(Int, TablePartitionStats)], (Int, TablePartitionStats)]): RDD[TablePartition] = {
    logDebug("To zip tachyon rdd")
    val rdd = prepareTablePartitionRDDBasedOnExistingRDD(newRdd, stat, oldRdd)
    writeTablePartitionRDDToTachyon(rdd)
  }



  protected[cloudstone] def prepareTablePartitionRDD(inputRdd: RDD[_],
                                                     stat: Accumulable[mutable.ArrayBuffer[(Int, TablePartitionStats)], (Int, TablePartitionStats)])
  : RDD[TablePartition] = {
    logDebug("rdd: " + inputRdd.toDebugString)
    inputRdd.mapPartitionsWithIndex { case(partitionIndex, iter) => {
      val tablePartitionBuilder = createPartitionBuilder()

      iter.foreach(row => {
        addRow(tablePartitionBuilder, row, createEventStructObjectInspector)
      })

      logDebug("Total row number: " + tablePartitionBuilder.numRows)
      if(stat!= null)stat += Tuple2(partitionIndex, tablePartitionBuilder.stats)
      Iterator(tablePartitionBuilder.build)
    }}
  }

  protected[cloudstone] def prepareTablePartitionRDDBasedOnExistingRDD(inputRdd: RDD[_],
                                                                       stat: Accumulable[mutable.ArrayBuffer[(Int, TablePartitionStats)], (Int, TablePartitionStats)],
                                                                       origRdd: RDD[_])
  : RDD[TablePartition] = {
    //only workable for the same partition number
    if(origRdd.partitions.length != inputRdd.partitions.length )
      throw new Exception("Non-equal partition number between newly input and original persisted RDDs");

    origRdd.asInstanceOf[RDD[TablePartition]].zipPartitions(inputRdd.asInstanceOf[RDD[Any]])(
      (oldRDDIter: Iterator[TablePartition], newRDDIter: Iterator[Any]) => {
        val tablePartitionBuilder = createPartitionBuilder()
        oldRDDIter.foreach(part => {
          part.iterator.foreach( row =>
            addRow(tablePartitionBuilder, row, createColumnarStructObjectInspector)
          ) })
        newRDDIter.foreach(row => addRow(tablePartitionBuilder, row, createEventStructObjectInspector))
        logDebug("Total row number: " + tablePartitionBuilder.numRows)
        Iterator(tablePartitionBuilder)
      })mapPartitionsWithIndex{ case(partitionIndex, iter) => {
        val partition = iter.next()
        if(stat!= null) stat += Tuple2(partitionIndex, partition.stats)
        Iterator(partition.build)
      }

    }
  }

  protected[cloudstone] def addRow(tablePartitionBuilder: TablePartitionBuilder ,
                                   row: Any,
                                   objectInspector: StructObjectInspector):TablePartitionBuilder = {
    val soi = objectInspector
    val fields: JList[_ <: StructField] = soi.getAllStructFieldRefs
    val obj =   row.asInstanceOf[Object]

    val currTm = System.currentTimeMillis()/1000
    val longInspector = PrimitiveObjInspectorFactory.newPrimitiveObjInspector("Long")

    val timeField = fields.get(timeColumnIndex)
    val timeFieldObjInspector = timeField.getFieldObjectInspector
    val recTm = soi.getStructFieldData(obj, timeField)

    val compareWithZero = ObjectInspectorUtils.compare(0L.asInstanceOf[Object], longInspector,
                                              recTm, timeFieldObjInspector)
    lazy val elapsedTime = ObjectInspectorUtils.compare(currTm.asInstanceOf[Object], longInspector,
      recTm, timeFieldObjInspector)

    if(compareWithZero != 0 && (cleanBefore == -1 || elapsedTime < cleanBefore)) {
      //append single row here
      logDebug("append one row here")
      for (i <- 0 until fields.size) {
        val field = fields.get(i)
        val fieldOI: ObjectInspector = field.getFieldObjectInspector
        fieldOI.getCategory match {
          case ObjectInspector.Category.PRIMITIVE =>
            tablePartitionBuilder.append(i, soi.getStructFieldData(obj, field), fieldOI)
          case other => {
            throw new Exception("Not support NonPrimitiveType currently")
          }
        }
      }
      tablePartitionBuilder.incrementRowCount()
    }
    logDebug("Row number: " + tablePartitionBuilder.numRows)
    tablePartitionBuilder
  }

  protected[cloudstone] def createEventStructObjectInspector :EventStructObjectInspector =
    new EventStructObjectInspector(this.fieldNames.toList, this.formats.toList)

  protected[cloudstone]  def createColumnarStructObjectInspector: ColumnarStructObjectInspector = {
    val columnNames = this.fieldNames.toList
    val columnTypes = this.formats.map(s=> {
      val pType = new PrimitiveTypeInfo
      pType.setTypeName(s.toLowerCase)
      pType
    }).toList

    val fields = new JArrayList[StructField]()
    for (i <- 0 until columnNames.size) {
      val typeInfo = columnTypes.get(i)
      val fieldOI = typeInfo.getCategory match {
        case Category.PRIMITIVE => PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
          typeInfo.asInstanceOf[PrimitiveTypeInfo].getPrimitiveCategory)
        case _ => throw new Exception("Not support NonPrimitiveType")
      }
      fields.add(new IDStructField(i, columnNames.get(i), fieldOI))
    }
    new ColumnarStructObjectInspector(fields)
  }

  protected[cloudstone] def createPartitionBuilder(shouldCompress: Boolean = false): TablePartitionBuilder =
    new TablePartitionBuilder(createColumnarStructObjectInspector, COLUMN_SIZE,
    shouldCompress)

  /**
   * Write the RDD onto Tachyon file system
   * @param inputrdd
   * @return
   */
  protected[cloudstone] def writeTablePartitionRDDToTachyon(inputrdd: RDD[TablePartition]): RDD[TablePartition] = {
    // Put the table in Tachyon.
    logDebug("Putting RDD for %s in Tachyon".format(outputName))

    if(!SharkEnv.tachyonUtil.tableExists(outputName)) {
      tachyonWriter.createTable(ByteBuffer.allocate(0))
    }

    inputrdd.mapPartitionsWithIndex {
      case(partitionIndex, iter) => {
        val partition = iter.next()
        writeColumnPartition(partition, partitionIndex)
        Iterator(partition)
      }
    }
  }

  protected[cloudstone] def writeColumnPartition(partition: TablePartition, partitionIndex: Int) {
    partition.toTachyon.zipWithIndex.foreach { case(buf, column) =>
      // we'd better to use the existing shark's API, but unfortunately the following function cannot overwrite the
      // existing partition files. Since the slave side cannot get rawTable without calling createTable()
      //tachyonWriter.writeColumnPartition(column, partitionIndex, buf)

      //the workaround solution
      val rawTable = SharkEnvSlave.tachyonUtil.client.getRawTable(SharkEnvSlave.tachyonUtil.getPath(outputName))
      val rawColumn = rawTable.getRawColumn(column)
      var file = rawColumn.getPartition(partitionIndex)
      if(file!=null && SharkEnvSlave.tachyonUtil.client.exist(file.getPath)) {
        //to delete the existing partition file before hand
        SharkEnvSlave.tachyonUtil.client.delete(file.getPath, true)
      }
      rawColumn.createPartition(partitionIndex)
      file = rawColumn.getPartition(partitionIndex)
      // it seems that CACHE_THROUGH will cause some tachyon exception
      // e.g., failed to rename some worker's checkpoint info to data folder
      // or asynchronously deletion
      val outStream = file.getOutStream(WriteType.TRY_CACHE)
      outStream.write(buf.array(), 0, buf.limit())
      outStream.close()
    }
  }

  protected[cloudstone] def readFromTachyon(tblName: String): RDD[TablePartition] = {
    if(!SharkEnv.tachyonUtil.tableExists(outputName)) null
    else {SharkEnv.tachyonUtil.createRDD(tblName)}
  }


}
