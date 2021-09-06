package io.rtdi.bigdata.kafka.avro.datatypes;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes.Decimal;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;

import io.rtdi.bigdata.kafka.avro.AvroDataTypeException;
import io.rtdi.bigdata.kafka.avro.AvroUtils;

/**
 * ENUM with all data types and their categorizations
 *
 */
public enum AvroType {
	/**
	 * A 8bit signed integer
	 */
	AVROBYTE(0, AvroDatatypeClass.NUMBER),
	/**
	 * ASCII text of large size, comparison and sorting is binary
	 */
	AVROCLOB(2, AvroDatatypeClass.TEXTASCII),
	/**
	 * Unicode text of large size, comparison and sorting is binary
	 */
	AVRONCLOB(4, AvroDatatypeClass.TEXTUNICODE),
	/**
	 * Unicode text up t n chars long, comparison and sorting is binary
	 */
	AVRONVARCHAR(3, AvroDatatypeClass.TEXTUNICODE),
	/**
	 * A 16bit signed integer
	 */
	AVROSHORT(1, AvroDatatypeClass.NUMBER),
	/**
	 * A Spatial data type in WKT representation 
	 */
	AVROSTGEOMETRY(0, AvroDatatypeClass.TEXTASCII),
	/**
	 * A Spatial data type in WKT representation 
	 */
	AVROSTPOINT(0, AvroDatatypeClass.TEXTASCII),
	/**
	 * A string as URI
	 */
	AVROURI(0, AvroDatatypeClass.TEXTASCII),
	/**
	 * An ASCII string of n chars length, comparison and sorting is binary
	 */
	AVROVARCHAR(1, AvroDatatypeClass.TEXTASCII),
	/**
	 * A date without time information 
	 */
	AVRODATE(0, AvroDatatypeClass.DATETIME),
	/**
	 * A numeric value with precision and scale
	 */
	AVRODECIMAL(6, AvroDatatypeClass.NUMBER),
	/**
	 * A time information down to milliseconds
	 */
	AVROTIMEMILLIS(0, AvroDatatypeClass.DATETIME),
	/**
	 * A time information down to microseconds
	 */
	AVROTIMEMICROS(1, AvroDatatypeClass.DATETIME),
	/**
	 * A timestamp down to milliseconds
	 */
	AVROTIMESTAMPMILLIS(2, AvroDatatypeClass.DATETIME),
	/**
	 * A timestamp down to microseconds
	 */
	AVROTIMESTAMPMICROS(3, AvroDatatypeClass.DATETIME),
	/**
	 * A timestamp down to milliseconds but without time zone
	 */
	AVROLOCALTIMESTAMPMILLIS(2, AvroDatatypeClass.DATETIME),
	/**
	 * A timestamp down to microseconds but without time zone
	 */
	AVROLOCALTIMESTAMPMICROS(3, AvroDatatypeClass.DATETIME),
	/**
	 * Boolean
	 */
	AVROBOOLEAN(0, AvroDatatypeClass.BOOLEAN),
	/**
	 * A 32bit signed integer value
	 */
	AVROINT(2, AvroDatatypeClass.NUMBER),
	/**
	 * A 64bit signed integer value
	 */
	AVROLONG(3, AvroDatatypeClass.NUMBER),
	/**
	 * A 32bit floating point number
	 */
	AVROFLOAT(4, AvroDatatypeClass.NUMBER),
	/**
	 * A 64bit floating point number
	 */
	AVRODOUBLE(5, AvroDatatypeClass.NUMBER),
	/**
	 * Binary data of any length
	 */
	AVROBYTES(1, AvroDatatypeClass.BINARY),
	/**
	 * A unbounded unicode text - prefer using nvarchar or nclob instead to indicate its normal length, comparison and sorting is binary
	 */
	AVROSTRING(5, AvroDatatypeClass.TEXTUNICODE),
	/**
	 * A binary object with an upper size limit
	 */
	AVROFIXED(0, AvroDatatypeClass.BINARY),
	/**
	 * A unicode string with a list of allowed values - one of enum(), comparison and sorting is binary
	 */
	AVROENUM(0, AvroDatatypeClass.TEXTUNICODE),
	/**
	 * A unicode string array with a list of allowed values - many of map(), comparison and sorting is binary
	 */
	AVROMAP(0, AvroDatatypeClass.COMPLEX),
	/**
	 * An ASCII string formatted as UUID, comparison and sorting is binary
	 */
	AVROUUID(0, AvroDatatypeClass.TEXTASCII),
	/**
	 * An array of elements
	 */
	AVROARRAY(1, AvroDatatypeClass.COMPLEX),
	/**
	 * A Record of its own
	 */
	AVRORECORD(0, AvroDatatypeClass.COMPLEX),
	/**
	 * An union of multiple primitive datatypes, e.g. used for extensions
	 */
	AVROANYPRIMITIVE(99, AvroDatatypeClass.TEXTUNICODE);
	
	private int level;
	private AvroDatatypeClass group;

	AvroType(int level, AvroDatatypeClass group) {
		this.level = level;
		this.group = group;
	}

	/**
	 * Take an Avro schema and try to find the best suited primitive data type.
	 * 
	 * @param schema the Avro schema for this data type, with or without logical type information
	 * @return the best suited AvroType or null
	 */
	public static AvroType getType(Schema schema) {
		LogicalType l = schema.getLogicalType();
		if (l != null) {
			switch (l.getName()) {
			case AvroByte.NAME: return AVROBYTE;
			case AvroCLOB.NAME: return AVROCLOB;
			case AvroNCLOB.NAME: return AVRONCLOB;
			case AvroNVarchar.NAME: return AVRONVARCHAR;
			case AvroShort.NAME: return AVROSHORT;
			case AvroSTGeometry.NAME: return AVROSTGEOMETRY;
			case AvroSTPoint.NAME: return AVROSTPOINT;
			case AvroUri.NAME: return AVROURI;
			case AvroVarchar.NAME: return AVROVARCHAR;
			case "date": return AVRODATE;
			case "decimal": return AVRODECIMAL;
			case "time-millis": return AVROTIMEMILLIS;
			case "time-micros": return AVROTIMEMICROS;
			case "timestamp-millis": return AVROTIMESTAMPMILLIS;
			case "timestamp-micros": return AVROTIMESTAMPMICROS;
			case "local-timestamp-millis": return AVROLOCALTIMESTAMPMILLIS;
			case "local-timestamp-micros": return AVROLOCALTIMESTAMPMICROS;
			case "uuid": return AVROUUID;
			}
		}
		switch (schema.getType()) {
		case BOOLEAN: return AVROBOOLEAN;
		case BYTES: return AVROBYTES;
		case DOUBLE: return AVRODOUBLE;
		case ENUM: return AVROENUM;
		case FIXED: return AVROFIXED;
		case FLOAT: return AVROFLOAT;
		case INT: return AVROINT;
		case LONG: return AVROLONG;
		case MAP: return AVROMAP;
		case STRING: return AVROSTRING;
		case ARRAY: return AVROARRAY;
		case RECORD: return AVRORECORD;
		case UNION: 
			if (schema.equals(AvroAnyPrimitive.getSchema())) {
				return AVROANYPRIMITIVE;
			}
		default: return null;
		}
	}

	/**
	 * @param schema the Avro schema for this data type, with or without logical type information
	 * @return the best suited AvroDataType or null
	 */
	public static IAvroDatatype getAvroDataType(Schema schema) {
        Schema baseschema = AvroUtils.getBaseSchema(schema);
		LogicalType l = baseschema.getLogicalType();
		if (l != null) {
			if (l instanceof IAvroDatatype) {
				return (IAvroDatatype) l;
			} else {
				switch (l.getName()) {
				case "date": return AvroDate.create();
				case "decimal": return AvroDecimal.create((Decimal) l);
				case "time-millis": return AvroTime.create();
				case "time-micros": return AvroTimeMicros.create();
				case "timestamp-millis": return AvroTimestamp.create();
				case "timestamp-micros": return AvroTimestampMicros.create();
				case "local-timestamp-millis": return AvroLocalTimestamp.create();
				case "local-timestamp-micros": return AvroLocalTimestampMicros.create();
				case "uuid": return AvroUUID.create();
				}
			}
		}
		switch (baseschema.getType()) {
		case BOOLEAN: return AvroBoolean.create();
		case BYTES: return AvroBytes.create();
		case DOUBLE: return AvroDouble.create();
		case ENUM: return AvroEnum.create(baseschema);
		case FIXED: return AvroFixed.create(baseschema);
		case FLOAT: return AvroFloat.create();
		case INT: return AvroInt.create();
		case LONG: return AvroLong.create();
		case MAP: return AvroMap.create(baseschema);
		case STRING: return AvroString.create();
		case ARRAY: return AvroArray.create(baseschema);
		case RECORD: return AvroRecord.create();
		case UNION: 
			if (schema.equals(AvroAnyPrimitive.getSchema())) {
				return AvroAnyPrimitive.create();
			}
		default: return null;
		}
	}
	
	/**
	 * Helper method to set a field value in a GenericRecord using the AvroDataType converter.
	 *  
	 * @param record to set the field in
	 * @param fieldname is the Avro-encoded name
	 * @param value a compatible value for this data type
	 * @throws AvroDataTypeException in case the field cannot be found, it has an unsupported data type or the provided value is not compatible
	 */
	public static void putRecordValue(GenericRecord record, String fieldname, Object value) throws AvroDataTypeException {
		Field f = record.getSchema().getField(fieldname);
		if (f == null) {
			throw new AvroDataTypeException("The field \"" + fieldname + "\" does not exist in the record \"" + record.getSchema().getName() + "\"");
		} else {
			IAvroDatatype dt = getAvroDataType(f.schema());
			if (dt == null) {
				throw new AvroDataTypeException("The field \"" + fieldname + "\" has a not supported type \"" + f.schema().getName() + "\"");
			} else {
				record.put(fieldname, dt.convertToInternal(value));
			}
		}
	}
	
	/**
	 * Helper method to create a GenericRecord for a field of type Record and assign it
	 * 
	 * @param record the parent record
	 * @param fieldname the name of the field
	 * @return GenericRecord to be used to
	 * @throws AvroDataTypeException in case the field cannot be found or is not a record type
	 */
	public static GenericRecord createChildRecordFor(GenericRecord record, String fieldname) throws AvroDataTypeException {
		Field f = record.getSchema().getField(fieldname);
		if (f == null) {
			throw new AvroDataTypeException("The field \"" + fieldname + "\" does not exist in the record \"" + record.getSchema().getName() + "\"");
		} else {
			Schema baseschema = AvroUtils.getBaseSchema(f.schema());
			if (baseschema.getType() != Type.RECORD) {
				throw new AvroDataTypeException("The field \"" + fieldname + "\" is of type \"" + baseschema.getName() + "\" and not a record");
			} else {
				Record r = new GenericData.Record(baseschema);
				record.put(fieldname, r);
				return r;
			}
		}
	}

	/**
	 * Helper method to create a GenericRecord and add it to the parent's field
	 * 
	 * @param record the parent record
	 * @param fieldname the name of the field
	 * @return GenericRecord to be used to
	 * @throws AvroDataTypeException in case the field cannot be found or is not a record type
	 */
	public static GenericRecord addChildToArrayOfRecords(GenericRecord record, String fieldname) throws AvroDataTypeException {
		Field f = record.getSchema().getField(fieldname);
		if (f == null) {
			throw new AvroDataTypeException("The field \"" + fieldname + "\" does not exist in the record \"" + record.getSchema().getName() + "\"");
		} else {
			Schema baseschema = AvroUtils.getBaseSchema(f.schema());
			if (baseschema.getType() != Type.ARRAY) {
				throw new AvroDataTypeException("The field \"" + fieldname + "\" is of type \"" + baseschema.getName() + "\" and not an array");
			} else {
				Schema arraytype = baseschema.getElementType();
				if (arraytype.getType() != Type.RECORD) {
					throw new AvroDataTypeException("The field \"" + fieldname + "\" is an array of \"" + arraytype.getName() + "\" and not an array of records");
				} else {
					Record r = new GenericData.Record(arraytype);
					@SuppressWarnings("unchecked")
					List<Record> l = (List<Record>) record.get(fieldname);
					if (l == null) {
						l = new ArrayList<Record>();
						record.put(fieldname, l);
					}
					l.add(r);
					return r;
				}
			}
		}
	}
	
	/**
	 * Helper method to return the best suited Java object for a field
	 * 
	 * @param record to read the field from
	 * @param fieldname to read the value from
	 * @return best suited Java object
	 * @throws AvroDataTypeException in case the field does not exist, has no supported logical data type or the conversion failed
	 */
	public static Object getRecordFieldValue(GenericRecord record, String fieldname) throws AvroDataTypeException {
		Field f = record.getSchema().getField(fieldname);
		if (f == null) {
			throw new AvroDataTypeException("The field \"" + fieldname + "\" does not exist in the record \"" + record.getSchema().getName() + "\"");
		} else {
			IAvroDatatype dt = getAvroDataType(f.schema());
			if (dt == null) {
				throw new AvroDataTypeException("The field \"" + fieldname + "\" has a not supported type \"" + f.schema().getName() + "\"");
			} else {
				Object v = record.get(fieldname);
				return dt.convertToJava(v);
			}
		}
	}
	
	/**
	 * Helper method to return the record of a field of type record
	 * 
	 * @param record to read the field from
	 * @param fieldname to read the record from
	 * @return a GenericRecord with the data
	 * @throws AvroDataTypeException in case the field does not exist, has no supported logical data type or the conversion failed
	 */
	public static GenericRecord getSubRecord(GenericRecord record, String fieldname) throws AvroDataTypeException {
		Field f = record.getSchema().getField(fieldname);
		if (f == null) {
			throw new AvroDataTypeException("The field \"" + fieldname + "\" does not exist in the record \"" + record.getSchema().getName() + "\"");
		} else {
			Schema baseschema = AvroUtils.getBaseSchema(f.schema());
			if (baseschema.getType() != Type.RECORD) {
				throw new AvroDataTypeException("The field \"" + fieldname + "\" is of type \"" + baseschema.getName() + "\" and not a record");
			} else {
				return (GenericRecord) record.get(fieldname);
			}
		}
	}

	/**
	 * Helper method to return the list of records of a field of type array-of-record
	 * 
	 * @param record to read the field from
	 * @param fieldname to read the record from
	 * @return a GenericRecord with the data
	 * @throws AvroDataTypeException in case the field does not exist, has no supported logical data type or the conversion failed
	 */
	public static List<GenericRecord> getSubRecordArray(GenericRecord record, String fieldname) throws AvroDataTypeException {
		Field f = record.getSchema().getField(fieldname);
		if (f == null) {
			throw new AvroDataTypeException("The field \"" + fieldname + "\" does not exist in the record \"" + record.getSchema().getName() + "\"");
		} else {
			Schema baseschema = AvroUtils.getBaseSchema(f.schema());
			if (baseschema.getType() != Type.ARRAY) {
				throw new AvroDataTypeException("The field \"" + fieldname + "\" is of type \"" + baseschema.getName() + "\" and not an array");
			} else {
				Schema arraytype = baseschema.getElementType();
				if (arraytype.getType() != Type.RECORD) {
					throw new AvroDataTypeException("The field \"" + fieldname + "\" is an array of \"" + arraytype.getName() + "\" and not an array of records");
				} else {
					@SuppressWarnings("unchecked")
					List<GenericRecord> l = (List<GenericRecord>) record.get(fieldname);
					return l;
				}
			}
		}
	}


	/**
	 * Take the Avro schema and return the best suited textual representation of this data type.
	 *   
	 * @param schema the Avro schema for this data type, with or without logical type information
	 * @return text representation of the best suited data type, e.g. VARCHAR(10)
	 */
	public static String getAvroDatatype(Schema schema) {
		if (schema.getType() == Type.UNION) {
			if (schema.getTypes().size() > 2) {
				return AvroAnyPrimitive.NAME;
			} else {
				schema = AvroUtils.getBaseSchema(schema);
			}
		}
		LogicalType l = schema.getLogicalType();
		if (l != null) {
			if (l instanceof LogicalTypeWithLength) {
				return l.toString();
			} else {
				return l.getName();
			}
		} else {
			return schema.getType().name();
		}
	}
	
	/**
	 * @param text the textual representation of a data type like VARCHAR(10)
	 * @return the Avro schema for this data type
	 */
	public static Schema getSchemaFromDataTypeRepresentation(String text) {
		switch (text) {
		case AvroBoolean.NAME: return AvroBoolean.getSchema();
		case AvroBytes.NAME: return AvroBytes.getSchema();
		case AvroDouble.NAME: return AvroDouble.getSchema();
		case AvroFloat.NAME: return AvroFloat.getSchema();
		case AvroInt.NAME: return AvroInt.getSchema();
		case AvroLong.NAME: return AvroLong.getSchema();
		case AvroString.NAME: return AvroString.getSchema();
		case AvroDate.NAME: return AvroDate.getSchema();
		case AvroTime.NAME: return AvroTime.getSchema();
		case AvroTimestamp.NAME: return AvroTimestamp.getSchema();
		case AvroLocalTimestamp.NAME: return AvroLocalTimestamp.getSchema();
		case AvroUUID.NAME: return AvroUUID.getSchema();
		case AvroAnyPrimitive.NAME: return AvroAnyPrimitive.getSchema();
		case AvroByte.NAME:
			return AvroByte.getSchema();
		case AvroCLOB.NAME:
			return AvroCLOB.getSchema();
		case AvroNCLOB.NAME:
			return AvroNCLOB.getSchema();
		case AvroShort.NAME:
			return AvroShort.getSchema();
		case AvroSTGeometry.NAME:
			return AvroSTGeometry.getSchema();
		case AvroSTPoint.NAME:
			return AvroSTPoint.getSchema();
		case AvroUri.NAME:
			return AvroUri.getSchema();
		}
		if (text.startsWith(AvroDecimal.NAME)) {
			return AvroDecimal.getSchema(text);
		} else if (text.startsWith(AvroNVarchar.NAME)) {
			return AvroNVarchar.getSchema(text);
		} else if (text.startsWith(AvroVarchar.NAME)) {
			return AvroVarchar.getSchema(text);
		} else {
			return null;
		}
	}

	/**
	 * @param text the textual representation of a data type like VARCHAR(10)
	 * @return the Avro data type for this data type
	 */
	public static IAvroDatatype getDataTypeFromString(String text) {
		if (text == null) {
			return null;
		}
		switch (text) {
		case AvroBoolean.NAME: return AvroBoolean.create();
		case AvroBytes.NAME: return AvroBytes.create();
		case AvroDouble.NAME: return AvroDouble.create();
		case AvroFloat.NAME: return AvroFloat.create();
		case AvroInt.NAME: return AvroInt.create();
		case AvroLong.NAME: return AvroLong.create();
		case AvroString.NAME: return AvroString.create();
		case AvroDate.NAME: return AvroDate.create();
		case AvroTime.NAME: return AvroTime.create();
		case AvroTimestamp.NAME: return AvroTimestamp.create();
		case AvroLocalTimestamp.NAME: return AvroLocalTimestamp.create();
		case AvroUUID.NAME: return AvroUUID.create();
		case AvroAnyPrimitive.NAME: return AvroAnyPrimitive.create();
		case AvroByte.NAME:
			return AvroByte.create();
		case AvroCLOB.NAME:
			return AvroCLOB.create();
		case AvroNCLOB.NAME:
			return AvroNCLOB.create();
		case AvroShort.NAME:
			return AvroShort.create();
		case AvroSTGeometry.NAME:
			return AvroSTGeometry.create();
		case AvroSTPoint.NAME:
			return AvroSTPoint.create();
		case AvroUri.NAME:
			return AvroUri.create();
		case AvroRecord.NAME:
			return AvroRecord.create();
		}
		if (text.startsWith(AvroDecimal.NAME)) {
			return AvroDecimal.create(text);
		} else if (text.startsWith(AvroNVarchar.NAME)) {
			return AvroNVarchar.create(text);
		} else if (text.startsWith(AvroVarchar.NAME)) {
			return AvroVarchar.create(text);
		} else {
			return null;
		}
	}

	/**
	 * @return the level attribute for this data type
	 */
	public int getLevel() {
		return level;
	}

	/**
	 * @return the group attribute for this data type
	 */
	public AvroDatatypeClass getGroup() {
		return group;
	}
	
	/**
	 * What is the best suited data type if the current data type needs to store more infomration?
	 * Example: The VARCHAR(10) can store ASCII chars only, but now Unicode is needed as well
	 * @param t extended type
	 * @return best suited AvroType
	 */
	public AvroType aggregate(AvroType t) {
		if (this == t) {
			return this;
		} else if (this.getGroup() == t.getGroup()) {
			// e.g. VARCHAR --> CLOB
			if (level < t.level) {
				return t;
			} else {
				return this;
			}
		} else {
			// e.g. CLOB --> NCLOB
			if (this.getGroup() == AvroDatatypeClass.TEXTASCII && t.getGroup() == AvroDatatypeClass.TEXTUNICODE) {
				if (this == AVROCLOB) {
					return AVRONCLOB;
				} else {
					return t;
				}
			} else {
				// e.g. DATE --> STRING
				return AVRONVARCHAR;
			}
		}
	}
	
	/**
	 * Based on the provided information return the best suited Avro data type.
	 * Default is a NVARCHAR(100).
	 * 
	 * @param type the AvroType or null
	 * @param length maximum length of the data type, ignored if it does not apply
	 * @param scale in case of a decimal, not used for all others
	 * @return the Avro data type
	 */
	public static IAvroDatatype getDataType(AvroType type, int length, int scale) {
		if (type == null) {
			return AvroNVarchar.create(100);
		} else {
			switch (type) {
			case AVROANYPRIMITIVE:
				return AvroAnyPrimitive.create();
			case AVROBOOLEAN:
				return AvroBoolean.create();
			case AVROBYTE:
				return AvroByte.create();
			case AVROBYTES:
				return AvroBytes.create();
			case AVROCLOB:
				return AvroCLOB.create();
			case AVRODATE:
				return AvroDate.create();
			case AVRODECIMAL:
				return AvroDecimal.create(length, scale);
			case AVRODOUBLE:
				return AvroDouble.create();
			case AVROFIXED:
				return AvroFixed.create(length);
			case AVROFLOAT:
				return AvroFloat.create();
			case AVROINT:
				return AvroInt.create();
			case AVROLONG:
				return AvroLong.create();
			case AVRONCLOB:
				return AvroCLOB.create();
			case AVRONVARCHAR:
				return AvroNVarchar.create(length);
			case AVRORECORD:
				return AvroRecord.create();
			case AVROSHORT:
				return AvroShort.create();
			case AVROSTGEOMETRY:
				return AvroSTGeometry.create();
			case AVROSTPOINT:
				return AvroSTPoint.create();
			case AVROSTRING:
				return AvroString.create();
			case AVROTIMEMICROS:
				return AvroTimeMicros.create();
			case AVROTIMEMILLIS:
				return AvroTime.create();
			case AVROTIMESTAMPMICROS:
				return AvroTimestampMicros.create();
			case AVROTIMESTAMPMILLIS:
				return AvroTimestamp.create();
			case AVROLOCALTIMESTAMPMICROS:
				return AvroLocalTimestampMicros.create();
			case AVROLOCALTIMESTAMPMILLIS:
				return AvroLocalTimestamp.create();
			case AVROURI:
				return AvroUri.create();
			case AVROUUID:
				return AvroUUID.create();
			case AVROVARCHAR:
				return AvroVarchar.create(length);
			default:
				return AvroNVarchar.create(length);
			}
		}
	}

}
