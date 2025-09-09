package io.rtdi.bigdata.kafka.avro.recordbuilders;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilderException;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;

import io.rtdi.bigdata.kafka.avro.AvroNameEncoder;

/**
 * Field in Avro
 */
public class AvroField extends Field {

	/**
	 * Data type information from the source system
	 */
	public static final String COLUMN_PROP_SOURCEDATATYPE = "__source_data_type";
	/**
	 * Original name of the field, because Avro has a limited set of characters allowed in field names
	 */
	public static final String COLUMN_PROP_ORIGINALNAME = "__originalname";
	/**
	 * Hint that this field is an internal field
	 */
	public static final String COLUMN_PROP_INTERNAL = "__internal";
	/**
	 * Cannot be used in mappings as the values are set when sending the rows to the pipeline server
	 */
	public static final String COLUMN_PROP_TECHNICAL = "__technical";
	/**
	 * Column sensitivity information, see {@link ContentSensitivity}
	 */
	public static final String COLUMN_PROP_CONTENT_SENSITIVITY = "__sensitivity";

	/**
	 * @param name of the field
	 * @param schema  of the field
	 * @param doc description
	 * @param nullable true if the field is optional
	 * @param defaultValue an optional default value
	 * @throws SchemaBuilderException in case of invalid combinations
	 */
	public AvroField(String name, Schema schema, String doc, boolean nullable, Object defaultValue) {
		super(AvroNameEncoder.encodeName(name), getSchema(schema, nullable), doc, defaultValue);
		setOriginalName(name);
	}

	/**
	 * @param name of the field
	 * @param schema  of the field
	 * @param doc description
	 * @param defaultValue an optional default value
	 * @param nullable true if the field is optional
	 * @param order Avro field order
	 * @throws SchemaBuilderException in case of invalid combinations
	 */
	public AvroField(String name, Schema schema, String doc, Object defaultValue, boolean nullable, Order order) {
		super(AvroNameEncoder.encodeName(name), getSchema(schema, nullable), doc, defaultValue, order);
		setOriginalName(name);
	}

	/**
	 * Create a new column with the contents based on a compiled schema field.
	 * Used to derive the key schema from the value schema.
	 *
	 * @param f as Avro's field object
	 */
	public AvroField(Field f) {
		super(f.name(), f.schema(), f.doc(), f.defaultVal(), f.order());
		setOriginalName(getOriginalName(f));
	}


	protected static Schema getSchema(Schema schema, boolean nullable) {
		if (nullable && schema.getType() != Type.UNION) { // a union of union is not supported
			return Schema.createUnion(Schema.create(Type.NULL), schema);
		} else {
			return schema;
		}
	}

	/**
	 * Set the source specific data type string - used as information only
	 *
	 * @param sourcedatatype any textual identifier for the source system data type
	 * @return this
	 */
	public AvroField setSourceDataType(String sourcedatatype) {
		addProp(COLUMN_PROP_SOURCEDATATYPE, sourcedatatype);
		return this;
	}

	/**
	 * Source specific data type string - used as information only
	 *
	 * @return the source data type identifier as specified
	 */
	public String getSourceDataType() {
		return getSourceDataType(this);
	}

	/**
	 * Define the security related sensitivity of this field.
	 *
	 * @param sensitivity of the field content
	 * @return this
	 */
	public AvroField setSensitivity(ContentSensitivity sensitivity) {
		addProp(COLUMN_PROP_CONTENT_SENSITIVITY, sensitivity.name());
		return this;
	}

	/**
	 * @return the field's content sensitivity
	 */
	public ContentSensitivity getSensitivity() {
		return getContentSensitivity(this);
	}

	/**
	 * The Avro field name has tight naming rules, less than what e.g. databases allow as field names
	 *
	 * @param name of the source field
	 */
	private void setOriginalName(String name) {
		addProp(COLUMN_PROP_ORIGINALNAME, name);
	}

	/**
	 * The Avro field name has tight naming rules, less than what e.g. databases allow as field names
	 *
	 * @return the name of the source field, usually the decoded Avro field name
	 */
	public String getOriginalName() {
		return getOriginalName(this);
	}

	/**
	 * Mark the field as an internal field, not one that is part of any official payload - source system id for example
	 *
	 * @return this
	 */
	public AvroField setInternal() {
		addProp(COLUMN_PROP_INTERNAL, Boolean.TRUE);
		return this;
	}

	/**
	 * Is this field marked as internal?
	 *
	 * @return true is the field is marked as internal
	 */
	public boolean isInternal() {
		return isInternal(this);
	}

	/**
	 * Mark the field as technical field, a field that is not part of the actual payload but contains some more data needed for other reasons
	 *
	 * @return this
	 */
	public AvroField setTechnical() {
		addProp(COLUMN_PROP_TECHNICAL, Boolean.TRUE);
		return this;
	}

	/**
	 * Is this a technical field?
	 *
	 * @return true if the field was marked as technical field
	 */
	public boolean isTechnical() {
		return isTechnical(this);
	}

	/**
	 * Method to extract the metadata from any field.
	 *
	 * @param field from which the metadata is to be read from
	 * @return the original source field name
	 */
	public static String getOriginalName(Field field) {
		Object name = field.getObjectProp(COLUMN_PROP_ORIGINALNAME);
		if (name != null && name instanceof String) {
			return (String) name;
		} else {
			return field.name();
		}
	}

	/**
	 * Method to extract the metadata from any field.
	 *
	 * @param field from which the metadata is to be read from
	 * @return the free text source data type identifier
	 */
	public static String getSourceDataType(Field field) {
		Object type = field.getObjectProp(COLUMN_PROP_SOURCEDATATYPE);
		if (type != null && type instanceof String) {
			return (String) type;
		} else {
			return null;
		}
	}

	/**
	 * Method to extract the metadata from any field.
	 *
	 * @param field from which the metadata is to be read from
	 * @return true in case it was marked as internal field
	 */
	public static boolean isInternal(Field field) {
		Object isinternal = field.getObjectProp(COLUMN_PROP_INTERNAL);
		if (isinternal != null && isinternal instanceof Boolean) {
			return (boolean) isinternal;
		} else {
			return false;
		}
	}

	/**
	 * Method to extract the metadata from any field.
	 *
	 * @param field from which the metadata is to be read from
	 * @return true in case it was marked as technical field
	 */
	public static boolean isTechnical(Field field) {
		Object istechnical = field.getObjectProp(COLUMN_PROP_TECHNICAL);
		if (istechnical != null && istechnical instanceof Boolean) {
			return (boolean) istechnical;
		} else {
			return false;
		}
	}

	/**
	 * Method to extract the metadata from any field.
	 *
	 * @param field from which the metadata is to be read from
	 * @return the sensitivity information set for this field or PUBLIC
	 */
	public static ContentSensitivity getContentSensitivity(Field field) {
		Object type = field.getObjectProp(COLUMN_PROP_CONTENT_SENSITIVITY);
		if (type != null && type instanceof String) {
			try {
				return ContentSensitivity.valueOf((String) type);
			} catch (IllegalArgumentException e) {
				return ContentSensitivity.PUBLIC;
			}
		} else {
			return ContentSensitivity.PUBLIC;
		}
	}

}
