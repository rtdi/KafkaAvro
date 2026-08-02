package io.rtdi.bigdata.kafka.avro.datatypes;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;

import io.rtdi.bigdata.kafka.avro.AvroDataTypeException;
import io.rtdi.bigdata.kafka.avro.recordbuilders.ValueSchema;

/**
 * The foundation of all Avro data types.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "type", visible = true)
@JsonSubTypes({
    @JsonSubTypes.Type(value = AvroAnyPrimitive.class, name = AvroAnyPrimitive.NAME),
    @JsonSubTypes.Type(value = AvroArray.class, name = AvroArray.NAME),
	@JsonSubTypes.Type(value = AvroBoolean.class, name = AvroBoolean.NAME),
	@JsonSubTypes.Type(value = AvroByte.class, name = AvroByte.NAME),
	@JsonSubTypes.Type(value = AvroBytes.class, name = AvroBytes.NAME),
	@JsonSubTypes.Type(value = AvroCLOB.class, name = AvroCLOB.NAME),
	@JsonSubTypes.Type(value = AvroDate.class, name = AvroDate.NAME),
	@JsonSubTypes.Type(value = AvroDecimal.class, name = AvroDecimal.NAME),
	@JsonSubTypes.Type(value = AvroDouble.class, name = AvroDouble.NAME),
	@JsonSubTypes.Type(value = AvroEnum.class, name = AvroEnum.NAME),
	@JsonSubTypes.Type(value = AvroFixed.class, name = AvroFixed.NAME),
	@JsonSubTypes.Type(value = AvroFloat.class, name = AvroFloat.NAME),
	@JsonSubTypes.Type(value = AvroInt.class, name = AvroInt.NAME),
	@JsonSubTypes.Type(value = AvroLocalTimestamp.class, name = AvroLocalTimestamp.NAME),
	@JsonSubTypes.Type(value = AvroLocalTimestampMicros.class, name = AvroLocalTimestampMicros.NAME),
	@JsonSubTypes.Type(value = AvroLong.class, name = AvroLong.NAME),
	@JsonSubTypes.Type(value = AvroMap.class, name = AvroMap.NAME),
	@JsonSubTypes.Type(value = AvroNCLOB.class, name = AvroNCLOB.NAME),
	@JsonSubTypes.Type(value = AvroNull.class, name = AvroNull.NAME),
	@JsonSubTypes.Type(value = AvroNVarchar.class, name = AvroNVarchar.NAME),
	@JsonSubTypes.Type(value = AvroShort.class, name = AvroShort.NAME),
	@JsonSubTypes.Type(value = AvroSTGeometry.class, name = AvroSTGeometry.NAME),
	@JsonSubTypes.Type(value = AvroSTPoint.class, name = AvroSTPoint.NAME),
	@JsonSubTypes.Type(value = AvroString.class, name = AvroString.NAME),
	@JsonSubTypes.Type(value = AvroTime.class, name = AvroTime.NAME),
	@JsonSubTypes.Type(value = AvroTimeMicros.class, name = AvroTimeMicros.NAME),
	@JsonSubTypes.Type(value = AvroTimestamp.class, name = AvroTimestamp.NAME),
	@JsonSubTypes.Type(value = AvroTimestampMicros.class, name = AvroTimestampMicros.NAME),
	@JsonSubTypes.Type(value = AvroUnion.class, name = AvroUnion.NAME),
	@JsonSubTypes.Type(value = AvroUri.class, name = AvroUri.NAME),
	@JsonSubTypes.Type(value = AvroUUID.class, name = AvroUUID.NAME),
	@JsonSubTypes.Type(value = AvroVarchar.class, name = AvroVarchar.NAME),
	@JsonSubTypes.Type(value = RecordSchema.class, name = RecordSchema.NAME),
	@JsonSubTypes.Type(value = ValueSchema.class, name = ValueSchema.NAME)
})
public interface IAvroDatatype {

	/**
	 * Helper method for toString
	 * 
	 * @param b the StringBuffer to append to
	 * @param value the value to convert to a string
	 */
	void toString(StringBuffer b, Object value);

	/**
	 * Get the type name of this data type.
	 * @return the type name
	 */
	public String getType();

	/**
	 * 
	 * The Avro Schema representation of this object
	 * @return the Avro Schema representation of this object
	 */
	public Schema createSchema();

	/**
	 * Convert a Java object to the compatible Java object expected by Avro.
	 * Example: Input is a string with TRUE/FALSE but the Avro data type is boolean, hence a Boolean is required.
	 *
	 * @param value any compatible input for this data type
	 * @return The Java object as expected by Avro
	 * @throws AvroDataTypeException in case the input value cannot be converted
	 */
	Object convertToInternal(Object value) throws AvroDataTypeException;

	/**
	 * What is the Avro backing data type?
	 * 
	 * @return the Avro expected data type
	 */
	@JsonIgnore
	Type getBackingType();

	/**
	 * The schema definition of this data type. This is the schema that is used to create the Avro schema for a record.
	 * 
	 * @return the full Avro schema definition needed for this datatype, e.g. String with length information
	 */
	@JsonIgnore
	Schema getDatatypeSchema();

	/**
	 * What is the Avro type of this data type?
	 * 
	 * @return the AvroType of this data type
	 */
	@JsonIgnore
	AvroType getAvroType();

	/**
	 * Convert the Avro value to the most logical Java object.
	 * Example: The Avro value is a Long but the data type a timestamp, hence a Java Instant (UTC timestamp) is returned.
	 *
	 * @param value Java object as read from the Record
	 * @return best suited Java data type representing this value
	 * @throws AvroDataTypeException if the conversion fails
	 */
	Object convertToJava(Object value) throws AvroDataTypeException;

	/**
	 * Convert the value to a JSON string representation.
	 * Useful in case a complex type like a record must be stored as JSON string.
	 *
	 * @param value Java object as read from the Record
	 * @return the JSON string representation of the value
	 * @throws AvroDataTypeException if the conversion fails
	 * @throws JsonProcessingException if the conversion to JSON fails
	 */
	String convertToJson(Object value) throws AvroDataTypeException, JsonProcessingException;
}
