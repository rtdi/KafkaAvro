package io.rtdi.bigdata.kafka.avro.recordbuilders;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilderException;

/**
 * Complex Avro type to represent an array of a given schema.
 */
public class AvroArray extends AvroField {

	/**
	 * Optional schema property to define the minimum number of entries in the array
	 */
	public static final String COLUMN_PROP_MIN = "__min";
	/**
	 * Optional schema property to define the maximum number of entries in the array
	 */
	public static final String COLUMN_PROP_MAX = "__max";

	private Schema schema;

	/**
	 * @param name of the field
	 * @param schema  of the field
	 * @param doc description
	 * @param defaultValue an optional default value
	 * @param order Avro field order
	 * @throws SchemaBuilderException in case of invalid combinations
	 */
	public AvroArray(String name, Schema schema, String doc, Object defaultValue, Order order) throws SchemaBuilderException {
		super(name, Schema.createArray(schema), doc, defaultValue, true, order);
		this.schema = schema;
	}

	/**
	 * @param name of the field
	 * @param schema  of the field
	 * @param doc description
	 * @param defaultValue an optional default value
	 * @throws SchemaBuilderException in case of invalid combinations
	 */
	public AvroArray(String name, Schema schema, String doc, Object defaultValue) throws SchemaBuilderException {
		super(name, Schema.createArray(schema), doc, true, defaultValue);
		this.schema = schema;
	}

	/**
	 * @return the schema of this logical type
	 */
	public Schema getArrayElementSchema() {
		return schema;
	}

	/**
	 * This setting allows database writers to optimize the handling. For example an address might have multiple lines
	 * hence it is an array of strings. But if we can agree that there will be never more than four lines, the database table
	 * could store it in up to four columns instead of using a separate table.<br>
	 * null means unbounded
	 * 0 means zero entries are allowed and hence can be used for min only
	 * 1 means that at least or at most one entry is needed if the array itself is present
	 *
	 * @param min allowed occurrences, null, 0 or any positive int
	 * @param max allowed occurrences, null, 1 or any larger positive int
	 * @return AvroArray object
	 * @throws SchemaBuilderException if min &gt; max or any other illegal value for min or max
	 */
	public AvroArray setMinMax(Integer min, Integer max) throws SchemaBuilderException {
		if (min != null) {
			if (min < 0) {
				throw new SchemaBuilderException("Min has to be apositive int or null, got \"" + String.valueOf(min) + "\"");
			}
			addProp(COLUMN_PROP_MIN, min);
		}
		if (max != null) {
			if (max < 1) {
				throw new SchemaBuilderException("Min has to be apositive int or null, got \"" + String.valueOf(max) + "\"");
			} else if (min != null && min > max) {
				throw new SchemaBuilderException("Min \"=" + String.valueOf(min) + "\" is greater than max \"=" + String.valueOf(max) + "\"");
			}
			addProp(COLUMN_PROP_MAX, max);
		}
		return this;
	}

	/**
	 * @param schema of the logical type
	 * @return the extracted __min property or null if there is none
	 */
	public static Integer getMin(Schema schema) {
		Object o = schema.getObjectProp(COLUMN_PROP_MIN);
		if (o instanceof Integer) {
			return (Integer) o;
		} else {
			return null;
		}
	}

	/**
	 * @param schema of the logical type
	 * @return the extracted __max property or null if there is none
	 */
	public static Integer getMax(Schema schema) {
		Object o = schema.getObjectProp(COLUMN_PROP_MAX);
		if (o instanceof Integer) {
			return (Integer) o;
		} else {
			return null;
		}
	}
}
