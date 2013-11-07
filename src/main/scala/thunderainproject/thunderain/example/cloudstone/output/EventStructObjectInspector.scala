package thunderainproject.thunderain.example.cloudstone.output

import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspectorUtils, ObjectInspector, StructField, StructObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category

import scala.reflect.BeanProperty

import java.util.{List => JList, ArrayList => JArrayList}

import thunderainproject.thunderain.framework.output.PrimitiveObjInspectorFactory
import thunderainproject.thunderain.framework.Event

/**
 * Parsing the structure in Event.
 *
 * Now it only supports those primitive types
 */
class EventStructObjectInspector extends StructObjectInspector{

  protected class MyField extends StructField {
    @BeanProperty var fieldID: Int = _
    @BeanProperty var fieldName: String = _
    @BeanProperty var fieldObjectInspector: ObjectInspector = _
    @BeanProperty var fieldComment: String = _

    def this(fieldID: Int, fieldName: String,
       fieldObjectInspector: ObjectInspector) {
      this()
      this.fieldID = fieldID
      this.fieldName = fieldName.toLowerCase()    // the column name must be in lowercase
      this.fieldObjectInspector = fieldObjectInspector
    }

     def this(fieldID: Int, fieldName: String,
       fieldObjectInspector: ObjectInspector, fieldComment: String) {
      this(fieldID, fieldName, fieldObjectInspector)
      this.fieldComment = fieldComment
    }

    override def toString(): String = {
      "" + fieldID + ":" + fieldName
    }
  }

  private var fields: JList[MyField] = _


  override def getTypeName(): String =  {
    ObjectInspectorUtils.getStandardStructTypeName(this)
  }

  def this(structFieldNames: JList[String],
            structFieldObjectInspectorsInString: JList[String]) {
    this()
    val structObjectInspectors = new JArrayList[ObjectInspector](structFieldNames.size())
    for(i <- 0 until structFieldNames.size) {
      structObjectInspectors.add(PrimitiveObjInspectorFactory.newPrimitiveObjInspector(
        structFieldObjectInspectorsInString.get(i)))
    }
    init(structFieldNames, structObjectInspectors, null)
  }

  protected def init(structFieldNames: JList[String] ,
    structFieldObjectInspectors: JList[ObjectInspector] ,
    structFieldComments: JList[String]) {
    assert (structFieldNames.size() == structFieldObjectInspectors.size())
    assert (structFieldComments == null ||
      (structFieldNames.size() == structFieldComments.size()))

    fields = new JArrayList[MyField](structFieldNames.size())
    for (i <- 0 until structFieldNames.size()) {
      fields.add(new MyField(i, structFieldNames.get(i),
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
