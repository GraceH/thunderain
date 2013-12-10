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
import shark.{SharkContext, SharkEnvSlave, SharkEnv}

import scala._
import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.concurrent._
import ExecutionContext.Implicits.global

import java.util.{List => JList, ArrayList => JArrayList}
import java.nio.ByteBuffer

import tachyon.client.WriteType

object TachyonRDDOutput extends Logging{
  //This function only for performance measurement.
  def elapsedTime[T](func: => T, actionName: String ) = {
    val startTime =  System.currentTimeMillis()
    val result = func
    val endTime = System.currentTimeMillis()
    logInfo("Action %s takes totally %d milli-seconds".format(actionName, endTime - startTime))
    result
  }
}

/**
 * Output the data structure to tachyon table
 * 1. get the format automatically from the meta info from shark
 * 2. ser/der the column accordingly onto the tachyon
 * 3. persist the meta-data(including the states for PartitionPrune) onto the tachyon for offline accessing
 * 4. merge the older data with newly updates(deleting obsolete data based on the DATA_TTL)
 *
 *
 */
class TachyonRDDOutput extends AbstractEventOutput with Logging{
  initLogging()

  var fieldNames: Array[String] = _

  var tachyonWriter: TachyonTableWriter = _
  var timeColumnIndex: Int = _
  var timestampFieldName: String = _

  val cleanBefore = if (System.getenv("DATA_CLEAN_TTL") == null) {
    -1
  } else {
    System.getenv("DATA_CLEAN_TTL").toLong * 1000L
  }

  var cachedRDD: RDD[TablePartition] = _

  val COLUMN_SIZE = 1000
  private var checkpointTm = System.currentTimeMillis() / 1000
  val CHECKPOINT_INTERVAL = 600

  /**
   * Set and parsing the output arguments accordingly
   * @param args
   */
  override def setArgs(args: String): Unit = {
    timestampFieldName = args.split(" ").apply(0)
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
  }

  protected[cloudstone] def setOutputFormat(fieldNames: Array[String], fieldFormats: Array[String]) {
    this.fieldNames = fieldNames
    super.setOutputDataFormat(fieldFormats)
  }

  override def setOutputDataFormat(formats: Array[String]) = ()

  override def preprocessOutput(stream: DStream[_]): DStream[_] = {

    val sc = try{
      stream.context.sparkContext.asInstanceOf[SharkContext]  }
    catch {
      case _ => {
        logError("Failed to obtain a SharkContext instance")
        null
      }
    }
    if(sc != null){
      val resultSets = sc.sql("describe %s".format(outputName)).flatMap(_.split("\\t")).zipWithIndex
      setOutputFormat(resultSets.filter(_._2%3==0).map(_._1).toArray,
        resultSets.filter(_._2%3==1).map(_._1).toArray)

      timeColumnIndex = fieldNames.zipWithIndex.toMap.apply(timestampFieldName)
    }

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
        val newRdd = try {zipTachyonRdd(oldRdd, r, statAccum)}
        catch{case _ => buildTachyonRdd(r, statAccum)}
        //checkpoint is not necessary for tachyon output
        if(cachedRDD != null) {
          val currTm = System.currentTimeMillis() / 1000
          if (currTm - checkpointTm >= CHECKPOINT_INTERVAL) {
            cachedRDD.checkpoint()
            checkpointTm = currTm
          }
        }
        newRdd
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
    TachyonRDDOutput.elapsedTime(inputRdd.cache(), "CachingInSpark")
    if(cachedRDD!=null)cachedRDD.unpersist()
    cachedRDD = inputRdd
    writeTablePartitionRDDToTachyon(inputRdd)
  }



  protected[cloudstone] def zipTachyonRdd(oldRdd: RDD[_], newRdd: RDD[_],
    stat: Accumulable[mutable.ArrayBuffer[(Int, TablePartitionStats)], (Int, TablePartitionStats)]): RDD[TablePartition] = {
    logDebug("To zip tachyon rdd")
    val rdd = prepareTablePartitionRDDBasedOnExistingRDD(newRdd, stat, oldRdd)
    TachyonRDDOutput.elapsedTime(rdd.cache(), "CachingInSpark")
    if(cachedRDD!=null)cachedRDD.unpersist()
    cachedRDD = rdd
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

    val currTm = System.currentTimeMillis()
    val longInspector = PrimitiveObjInspectorFactory.newPrimitiveObjInspector("Long")

    val timeField = fields.get(timeColumnIndex)
    val timeFieldObjInspector = timeField.getFieldObjectInspector
    val recTm = soi.getStructFieldData(obj, timeField)

    val compareWithZero = ObjectInspectorUtils.compare(0L.asInstanceOf[Object], longInspector,
                                              recTm, timeFieldObjInspector)
    lazy val elapsedTime = ObjectInspectorUtils.copyToStandardJavaObject(
                              currTm.asInstanceOf[Object], longInspector).asInstanceOf[Long] -
                           ObjectInspectorUtils.copyToStandardJavaObject(
                              recTm, timeFieldObjInspector).asInstanceOf[Long]

    //logDebug("elaspedTime: " + elapsedTime + " cleanBefore: " + cleanBefore + " isZero: " + compareWithZero)
    if(compareWithZero != 0 && (cleanBefore == -1 || elapsedTime < cleanBefore)) {
      //append single row here
      //logDebug("append one row here")
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
    //logDebug("Row number: " + tablePartitionBuilder.numRows)
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
        val startTime = System.currentTimeMillis()
        writeColumnPartition(partition, partitionIndex)
        val endTime = System.currentTimeMillis()
        logInfo("Write Partition# %d  onto Tachyon costs: %d ms.".format(partitionIndex,(endTime - startTime)))
        Iterator(partition)
      }
    }
  }

  protected[cloudstone] def writeColumnPartition(partition: TablePartition, partitionIndex: Int) {
    partition.toTachyon.zipWithIndex.foreach { case(buf, column) =>
      val  f: Future[Unit] = future {
        //write output to tachyon in parallel
        //TODO to cache the exception and register some onSuccess or onFailure actions.

        // we'd better to use the existing shark's API, but unfortunately the following function cannot overwrite the
        // existing partition files. Since the slave side cannot get rawTable without calling createTable()
        //tachyonWriter.writeColumnPartition(column, partitionIndex, buf)

        val startTime = System.currentTimeMillis()
        //the workaround solution
        val tablepath = SharkEnvSlave.tachyonUtil.getPath(outputName)
        val rawTable = SharkEnvSlave.tachyonUtil.client.getRawTable(tablepath)
        val rawColumn = rawTable.getRawColumn(column)
        var file = rawColumn.getPartition(partitionIndex)
        if(file!=null && SharkEnvSlave.tachyonUtil.client.exist(file.getPath)) {
          //to delete the existing partition file before hand
          //TODO this solution will cause shark query failure!!!!
          SharkEnvSlave.tachyonUtil.client.delete(file.getPath, true)
        }
        rawColumn.createPartition(partitionIndex)
        file = rawColumn.getPartition(partitionIndex)
        //TODO catch exception when caching data
        val outStream = file.getOutStream(WriteType.CACHE_THROUGH)
        try{
          outStream.write(buf.array(), 0, buf.limit())
        }
        finally {
          //to close the outStream in any case
          if(outStream != null) outStream.close()
        }
        val endTime = System.currentTimeMillis()
        logInfo("Write Col# %d Partition# %d  onto Tachyon costs: %d ms.".format(column, partitionIndex,(endTime - startTime)))
      }}
  }

  protected[cloudstone] def readFromTachyon(tblName: String): RDD[TablePartition] = {
    if (cachedRDD != null) cachedRDD
    if(!SharkEnv.tachyonUtil.tableExists(outputName)) null
    else {SharkEnv.tachyonUtil.createRDD(tblName)}
  }


}
