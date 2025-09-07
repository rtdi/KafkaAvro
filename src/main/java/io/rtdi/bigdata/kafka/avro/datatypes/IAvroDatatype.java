package io.rtdi.bigdata.kafka.avro.datatypes;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.rtdi.bigdata.kafka.avro.AvroDataTypeException;

/**
 * The foundation of all Avro data types.
 */
public interface IAvroDatatype {

	/**
	 * @param b the StringBuffer to append to
	 * @param value the value to convert to a string
	 */
	void toString(StringBuffer b, Object value);

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
	 * @return the Avro expected data type
	 */
	Type getBackingType();

	/**
	 * @return the full Avro schema definition needed for this datatype, e.g. String with length information
	 */
	Schema getDatatypeSchema();

	/**
	 * @return the AvroType of this data type
	 */
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
