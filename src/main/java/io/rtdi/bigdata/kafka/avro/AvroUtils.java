package io.rtdi.bigdata.kafka.avro;

import java.util.HashSet;
import java.util.List;

import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.commons.text.StringEscapeUtils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * Some utility functions for Avro
 */
public class AvroUtils {

	/**
	 * Executes the ObjectMapper createJacksonOM operation and returns the resulting value.
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
	/**
	 * Executes the String encodeJson operation and returns the resulting value.
	 * @param text the parameter value
	 * @return the resulting value
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
	/**
	 * Executes the Schema getBaseSchema operation and returns the resulting value.
	 * @param schema the parameter value
	 * @return the resulting value
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

	@SuppressWarnings("unchecked")
	/**
	 * Executes the boolean isEqual operation and returns the resulting value.
	 * @param a the parameter value
	 * @param b the parameter value
	 * @return the resulting value
	 */
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
	 * Executes the String nullif operation and returns the resulting value.
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
}
