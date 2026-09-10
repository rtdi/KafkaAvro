package io.rtdi.bigdata.kafka.avro;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.AvroTypeException;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.commons.text.StringEscapeUtils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * Some utility functions for Avro
 */
public class AvroUtils {

	/**
	 * Creates a new instance of this class.
	 */
	public AvroUtils() {
		super();
	}
	/**
	 * Create an object mapper for Jackson.
	 * @return the resulting value
	 */
	public static ObjectMapper createJacksonOM() {
		ObjectMapper om = new ObjectMapper().disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
		om.setDefaultPropertyInclusion(
			JsonInclude.Value.construct(
				JsonInclude.Include.NON_NULL,
				JsonInclude.Include.NON_NULL
			)
		);
		return om;
	}
	/**
	 * Convert a text into a string used as value for a Json field.
	 *
	 * @param text input text to be escaped
	 * @return properly escaped string so it does not break the Json format
	 */
	public static String encodeJson(String text) {
		/*
		 * Backspace is replaced with \b
		 * Form feed is replaced with \f
		 * Newline is replaced with \n
		 * Carriage return is replaced with \r
		 * Tab is replaced with \t
		 * Double quote is replaced with \"
		 * Backslash is replaced with \\
		 */
		return StringEscapeUtils.escapeJson(text);
	}

	/**
	 * Avro binary messages as stored in Kafka with Schema Registry have a magic byte at the beginning of the message
	 */
	public static final byte MAGIC_BYTE = 0x0;

	/**
	 * In case this schema is a union of null and something else, it returns the _something else_
	 *
	 * @param schema of the input
	 * @return schema without the union of null, in case it is just that. Can return an union still.
	 */
	public static Schema getBaseSchema(Schema schema) {
		if (schema == null) {
			return null;
		} else if (schema.getType() == Type.UNION) {
			List<Schema> types = schema.getTypes();
			/*
			 * The first element is what is the data type used by the default value, hence it can be at both places
			 */
			if (types.size() == 2 && types.get(0).getType() == Type.NULL) { // union of null and something else, e.g. ["null", "string"]
				return types.get(1);
			} else if (types.size() == 2 && types.get(1).getType() == Type.NULL) { // union of something plus null, e.g. ["string"], "null"
				return types.get(0);
			} else if (types.size() == 1) { // union of a single type, e.g. ["string"]
				return types.get(0);
			} else {
				return schema;
			}
		} else {
			return schema;
		}

	}

	/**
	 * Compare two objects gracefully
	 * @param a first object to compare
	 * @param b second object to compare
	 * @return true if the objects are equal, false otherwise
	 */
	@SuppressWarnings("unchecked")
	public static boolean isEqual(Object a, Object b) {
		if (a == null) {
			return b == null || JsonProperties.NULL_VALUE.equals(b);
		} else if (b == null) {
			return JsonProperties.NULL_VALUE.equals(a);
		} else if (a instanceof List c && b instanceof List d) {
			return new HashSet<>(c).equals(new HashSet<>(d));
		} else if (a instanceof Number c && b instanceof Number d) {
			return c.intValue() == d.intValue();
		} else {
			return a.equals(b);
		}
	}

	/**
	 * Return null if the string is of length zero
	 * @param text the parameter value
	 * @return the resulting value
	 */
	public static String nullif(String text) {
		if (text == null) {
			return null;
		} else if (text.isEmpty()) {
			return null;
		} else {
			return text;
		}
	}

	/**
	 * Safely convert a object into a list, validating the types of the provided object and its elements
	 * @param <E> Type of the list element
	 * @param o object to convert
	 * @param clazz Class of the list element
	 * @return input o but of the correct type
	 */
	@SuppressWarnings("unchecked")
	public static <E> List<E> castListType(Object o, Class<E> clazz) {
		if (o == null) {
			return null;
		} else if (o instanceof List<?> l) {
			for (Object obj : l) {
				if (!obj.getClass().isAssignableFrom(clazz)) {
					throw new AvroTypeException("The provided data does not match the expected list type");
				}
			}
			return (List<E>) o;
		} else {
			throw new AvroTypeException("The provided object is not a list");
		}
	}

	/**
	 * Validate the object is a list of Avro records.
	 * @param o object to convert
	 * @return input o but of the correct type
	 */
	@SuppressWarnings("unchecked")
	public static List<GenericData.Record> castListOfRecords(Object o) {
		if (o == null) {
			return null;
		} else if (o instanceof List<?> l) {
			for (Object obj : l) {
				if (!(obj instanceof GenericData.Record)) {
					throw new AvroTypeException("The provided list contains elements other than an Avro Record");
				}
			}
			return (List<GenericData.Record>) o;
		} else {
			throw new AvroTypeException("The provided object is not a list");
		}
	}

	/**
	 * Validate the object is a map of Avro records.
	 * @param o object to convert
	 * @return input o but of the correct type
	 */
	@SuppressWarnings("unchecked")
	public static Map<String, GenericData.Record> castMapOfRecords(Object o) {
		if (o == null) {
			return null;
		} else if (o instanceof Map<?, ?> l) {
			for ( Entry<?, ?> obj : l.entrySet()) {
				if (!(obj.getValue() instanceof GenericData.Record)) {
					throw new AvroTypeException("The provided map value contains elements other than an Avro Record");
				} else if (!(obj.getKey() instanceof String)) {
					throw new AvroTypeException("The provided map key contains elements other than a String");
				}
			}
			return (Map<String, GenericData.Record>) o;
		} else {
			throw new AvroTypeException("The provided object is not a map");
		}
	}

	/**
	 * Validate the object is a map.
	 * @param o object to convert
	 * @param clazz Class of the map value
	 * @param <T> Type of the map value
	 * @return input o but of the correct type
	 */
	@SuppressWarnings("unchecked")
	public static <T> Map<String, T> castMap(Object o, Class<T> clazz) {
		if (o == null) {
			return null;
		} else if (o instanceof Map<?, ?> l) {
			for (Entry<?, ?> obj : l.entrySet()) {
				if (!(!o.getClass().isAssignableFrom(clazz))) {
					throw new AvroTypeException("The provided map value contains elements other than an the request type");
				} else if (!(obj.getKey() instanceof String)) {
					throw new AvroTypeException("The provided map key contains elements other than a String");
				}
			}
			return (Map<String, T>) o;
		} else {
			throw new AvroTypeException("The provided object is not a map");
		}
	}

	/**
	 * Validate the input Avro value is the correct data type
	 * @param <T> expected type
	 * @param o input value
	 * @param clazz of the expected type
	 * @return o but validated
	 */
	@SuppressWarnings("unchecked")
	public static <T> T castType(Object o, Class<T> clazz) throws AvroTypeException {
		if (o == null) {
			return null;
		} else if (!o.getClass().isAssignableFrom(clazz)) {
			throw new AvroTypeException("The provided object does not match the expected data type");
		} else {
			return (T) o;
		}
	}

	/**
	 * Validate the input Avro value is the correct data type
	 * @param o input value
	 * @return o casted to Recor
	 */
	public static GenericData.Record castAsRecord(Object o) throws AvroTypeException {
		if (o == null) {
			return null;
		} else if (!(o instanceof GenericData.Record)) {
			throw new AvroTypeException("The provided object does not match the expected data type");
		} else {
			return (GenericData.Record) o;
		}
	}


	/**
	 * Get from the record the field with the validated data type
	 * 
	 * @param <T> expected type
	 * @param data input record
	 * @param fieldname name fo the field
	 * @param clazz of the expected type
	 * @return the value
	 */
	public static <T> T getAvroValue(GenericData.Record data, String fieldname, Class<T> clazz) {
		if (data == null) {
			return null;
		} else {
			return castType(data.get(fieldname), clazz);
		}
	}

	/**
	 * Get from the record a list of strings
	 * 
	 * @param data input record
	 * @param fieldname name fo the field
	 * @return the value
	 */
	public static List<String> getAvroListOfString(GenericData.Record data, String fieldname) {
		if (data == null) {
			return null;
		} else {
			return castListType(data.get(fieldname), String.class);
		}
	}

	/**
	 * Get from the record a list of Integers
	 * 
	 * @param data input record
	 * @param fieldname name fo the field
	 * @return the value
	 */
	public static List<Integer> getAvroListOfInteger(GenericData.Record data, String fieldname) {
		if (data == null) {
			return null;
		} else {
			return castListType(data.get(fieldname), Integer.class);
		}
	}


	/**
	 * Get from the record the field with a list of records
	 * 
	 * @param data input record
	 * @param fieldname name fo the field
	 * @return the value
	 */
	public static List<GenericData.Record> getAvroListOfRecords(GenericData.Record data, String fieldname) {
		if (data == null) {
			return null;
		} else {
			return castListOfRecords(data.get(fieldname));
		}
	}

	/**
	 * Get from the record the field with record data
	 * 
	 * @param data input record
	 * @param fieldname name fo the field
	 * @return the value
	 */
	public static GenericData.Record getAvroRecord(GenericData.Record data, String fieldname) {
		if (data == null) {
			return null;
		} else {
			return castAsRecord(data.get(fieldname));
		}
	}

	/**
	 * Get from the record the field with a map of records
	 * 
	 * @param data input record
	 * @param fieldname name fo the field
	 * @return the value
	 */
	public static Map<String, GenericData.Record> getAvroMapOfRecords(GenericData.Record data, String fieldname) {
		if (data == null) {
			return null;
		} else {
			return castMapOfRecords(data.get(fieldname));
		}
	}

	/**
	 * Get from the record the field with a map
	 * 
	 * @param data input record
	 * @param fieldname name fo the field
	 * @param clazz Class of the map value
	 * @param <T> Type of the map value
	 * @return the value
	 */
	public static <T> Map<String, T> getAvroMap(GenericData.Record data, String fieldname, Class<T> clazz) {
		if (data == null) {
			return null;
		} else {
			return castMap(data.get(fieldname), clazz);
		}
	}

}
