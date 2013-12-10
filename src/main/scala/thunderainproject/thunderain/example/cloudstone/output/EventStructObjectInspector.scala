package thunderainproject.thunderain.example.cloudstone.output

import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspectorUtils, ObjectInspector, StructField, StructObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category

import scala.reflect.BeanProperty

import java.util.{List => JList, ArrayList => JArrayList}

import thunderainproject.thunderain.framework.output.PrimitiveObjInspectorFactory
import thunderainproject.thunderain.framework.Event
import shark.memstore2.ColumnarStructObjectInspector.IDStructField
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import scala._

/**
 * Parsing the structure in Event.
 *
 * Now it only supports those primitive types
 */
class EventStructObjectInspector extends StructObjectInspector{

  private var fields: JList[IDStructField] = _


  override def getTypeName(): String =  {
    ObjectInspectorUtils.getStandardStructTypeName(this)
  }

  def this(structFieldNames: JList[String],
            structFieldObjectInspectorsInString: JList[String]) {
    this()
    val structObjectInspectors = new JArrayList[ObjectInspector](structFieldNames.size())
    for(i <- 0 until structFieldNames.size) {
      val pType = new PrimitiveTypeInfo
      pType.setTypeName(structFieldObjectInspectorsInString.get(i))
      val fieldOI =  pType.getCategory match {
        case Category.PRIMITIVE =>  PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
          pType.getPrimitiveCategory)
        case _ => throw new Exception("Not support NonPrimitiveType")
      }
      structObjectInspectors.add(fieldOI)
    }
    init(structFieldNames, structObjectInspectors, null)
  }

  protected def init(structFieldNames: JList[String] ,
    structFieldObjectInspectors: JList[ObjectInspector] ,
    structFieldComments: JList[String]) {
    assert (structFieldNames.size() == structFieldObjectInspectors.size())
    assert (structFieldComments == null ||
      (structFieldNames.size() == structFieldComments.size()))

    fields = new JArrayList[IDStructField](structFieldNames.size())
    for (i <- 0 until structFieldNames.size()) {
      fields.add(new IDStructField(i, structFieldNames.get(i),
        structFieldObjectInspectors.get(i),
        if(structFieldComments == null)null else structFieldComments.get(i)))
    }
  }

  override def getCategory(): Category = {
    Category.STRUCT
  }

  // Without Data
  override def getStructFieldRef(fieldName: String): StructField =  {
    ObjectInspectorUtils.getStandardStructFieldRef(fieldName, fields)
  }


  override def getAllStructFieldRefs: JList[_ <: StructField] = {
    this.fields
  }

  /**
   * Data should be a Event which consists of tuples of ("fieldname", "data")
   * @param data
   * @param fieldRef
   * @return
   */
  override def getStructFieldData(data: Object,  fieldRef: StructField):Object =  {
    val keyMap = data.asInstanceOf[Event].keyMap
    val value = keyMap.getOrElse(fieldRef.getFieldName, null)
    if (value != null) PrimitiveObjInspectorFactory.stringObjConversion(value,
      fieldRef.getFieldObjectInspector.getTypeName)
      .asInstanceOf[Object]
    else value
  }

  /**
   * Data should be a Event which consists of tuples of ("fieldname", "data")
   * @param data
   * @return
   */
  override def getStructFieldsDataAsList(data: Object):JList[Object] = {
    val keyMap = data.asInstanceOf[Event].keyMap
    val objList = new JArrayList[Object](keyMap.size)
    for(i <- 0 until keyMap.size) {
      val fieldRef = getStructFieldRef(keyMap.toArray.apply(i)._1)

      objList.add(PrimitiveObjInspectorFactory.stringObjConversion(keyMap(fieldRef.getFieldName),
        fieldRef.getFieldObjectInspector.getTypeName)
        .asInstanceOf[Object])
    }
    objList
  }

}
