package io.rtdi.bigdata.kafka.avro.datatypes;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.Schema.Field;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.rtdi.bigdata.kafka.avro.AvroNameEncoder;
import io.rtdi.bigdata.kafka.avro.AvroUtils;
import io.rtdi.bigdata.kafka.avro.objects.ContentSensitivity;

/**
 * Field in Avro
 */
public class AvroField {

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
	 * Column semantic information, see {@link ColumnSemantic}
	 */
	public static final String COLUMN_PROP_SEMANTICS = "semantics";


	private String name;
	private String doc;
	private Object defaultValue;
	private boolean nullable = true;
	private String originalname;
	private String sourcedatatype;
	private ContentSensitivity sensitivity;
	private Boolean isinternal;
	private Boolean istechnical;
	private ColumnSemantics semantics;
	private IAvroDatatype datatype;
	private ObjectMapper om = AvroUtils.createJacksonOM();

	/**
	 * Creates a new instance of this class.
	 */
	public AvroField() {
		super();
	}

	/**
	 * @param name of the field
	 * @param datatype of the field
	 * @param doc description
	 * @param nullable true if the field is optional
	 * @param defaultValue an optional default value
	 * @throws SchemaBuilderException in case of invalid combinations
	 */
	/**
	 * Creates a new instance of this class.
	 * @param name the parameter value
	 * @param datatype the parameter value
	 * @param doc the parameter value
	 * @param nullable the parameter value
	 * @param defaultValue the parameter value
	 */
	public AvroField(String name, IAvroDatatype datatype, String doc, boolean nullable, Object defaultValue) {
		this(name, datatype, doc, nullable);
		this.defaultValue = defaultValue;
	}

	/**
	 * Creates a new instance of this class.
	 * @param name the parameter value
	 * @param datatype the parameter value
	 * @param doc the parameter value
	 * @param nullable the parameter value
	 */
	public AvroField(String name, IAvroDatatype datatype, String doc, boolean nullable) {
		this(name, datatype, doc);
		this.nullable = nullable;
		if (nullable) {
			this.defaultValue = JsonProperties.NULL_VALUE;
		}
	}
	/**
	 * Creates a new instance of this class.
	 * @param name the parameter value
	 * @param datatype the parameter value
	 * @param doc the parameter value
	 */
	public AvroField(String name, IAvroDatatype datatype, String doc) {
		this(name, datatype);
		this.doc = doc;
	}

	/**
	 * Creates a new instance of this class.
	 * @param name the parameter value
	 * @param datatype the parameter value
	 */
	public AvroField(String name, IAvroDatatype datatype) {
		this.name = AvroNameEncoder.encodeName(name);
		this.originalname = name;
		this.datatype = datatype;
		this.doc = null;
		this.defaultValue = JsonProperties.NULL_VALUE;
		this.nullable = true;
	}

	/**
	 * create a field based on another field
	 * @param avrofield the parameter value
	 * @return the resulting value
	 */
	public static AvroField create(Field avrofield) {
		IAvroDatatype datatype = AvroType.getAvroDataType(avrofield.schema());
		boolean nullable = false;
		if (avrofield.schema().getType() == Type.UNION) {
			Schema u = avrofield.schema();
			List<Schema> types = u.getTypes();
			if (types.size() == 2 && types.get(0).getType() == Type.NULL) { // union of null and something else, e.g. ["null", "string"]
				nullable = true;
			}
		}
		AvroField f = new AvroField();
		f.setName(avrofield.name());
		f.setDatatype(datatype);
		f.setDoc(avrofield.doc());
		f.setNullable(nullable);
		f.setDefaultValue(avrofield.defaultVal());
		String originalname = AvroType.getProp(avrofield, COLUMN_PROP_ORIGINALNAME, String.class);
		if (originalname != null) {
			f.setOriginalName(originalname);
		} else {
			f.setOriginalName(AvroNameEncoder.decodeName(avrofield.name()));
		}
		f.setSourceDataType(AvroType.getProp(avrofield, COLUMN_PROP_SOURCEDATATYPE, String.class));
		f.setSensitivity(AvroType.getProp(avrofield, COLUMN_PROP_CONTENT_SENSITIVITY, ContentSensitivity.class));
		f.setInternal(AvroType.getProp(avrofield, COLUMN_PROP_INTERNAL, Boolean.class));
		f.setTechnical(AvroType.getProp(avrofield, COLUMN_PROP_TECHNICAL, Boolean.class));
		f.setSemantics(AvroType.getProp(avrofield, COLUMN_PROP_SEMANTICS, ColumnSemantics.class));
		return f;
	}

	@JsonIgnore
	/**
	 * Constructs the schema
	 * @return the Avro field
	 */
	public Field getAvroField() {
		Schema fieldSchema = datatype.createSchema();
		Field f = null;
		if (nullable && fieldSchema.getType() != Type.UNION) { // a union of union is not supported
			f = new Field(name, Schema.createUnion(Schema.create(Type.NULL), fieldSchema), doc, JsonProperties.NULL_VALUE);
		} else {
			f = new Field(name, fieldSchema, doc, defaultValue);
		}
		addProp(f, COLUMN_PROP_ORIGINALNAME, originalname);
		addProp(f, COLUMN_PROP_SOURCEDATATYPE, sourcedatatype);
		addProp(f, COLUMN_PROP_CONTENT_SENSITIVITY, sensitivity);
		addProp(f, COLUMN_PROP_INTERNAL, isinternal);
		addProp(f, COLUMN_PROP_TECHNICAL, istechnical);
		addProp(f, COLUMN_PROP_SEMANTICS, semantics);
		return f;
	}

	private void addProp(Field f, String name, Object value) {
		if (value != null) {
			if (value == JsonProperties.NULL_VALUE) {
				f.addProp(name, value);
			} else if (value instanceof Map) { // record, map
				f.addProp(name, om.valueToTree(value));
			} else if (value instanceof Collection) { // array
				f.addProp(name, om.valueToTree(value));
			} else if (value instanceof byte[]) { // bytes, fixed
				f.addProp(name, value);
			} else if (value instanceof CharSequence || value instanceof Enum<?>) { // string, enum
				f.addProp(name, value);
			} else if (value instanceof Double) { // double
				f.addProp(name, value);
			} else if (value instanceof Float) { // float
				f.addProp(name, value);
			} else if (value instanceof Long) { // long
				f.addProp(name, value);
			} else if (value instanceof Integer) { // int
				f.addProp(name, value);
			} else if (value instanceof Boolean) { // boolean
				f.addProp(name, value);
			} else if (value instanceof BigInteger) {
				f.addProp(name, value);
			} else if (value instanceof BigDecimal) {
				f.addProp(name, value);
			} else {
				f.addProp(name, om.valueToTree(value));
			}
		}
	}

	/**
	 * get the datatype of the field
	 * @return the the datatype of the field
	 */
	public IAvroDatatype getDatatype() {
		return datatype;
	}

	/**
	 * Sets the datatype of the field.
	 * @param schema the parameter value
	 */
	public void setDatatype(IAvroDatatype schema) {
		this.datatype = schema;
	}

	/**
	 * get the name of the field
	 * @return the name of the field
	 */
	public String getName() {
		return name;
	}

	/**
	 * Sets the name of the field.
	 * @param name the parameter value
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * get the documentation of the field
	 * @return the documentation of the field
	 */
	public String getDoc() {
		return doc;
	}

	/**
	 * Sets the documentation of the field.
	 * @param doc the parameter value
	 */
	public void setDoc(String doc) {
		this.doc = doc;
	}

	/**
	 * get the default value of the field
	 * @return the default value of the field
	 */
	public Object getDefaultValue() {
		return defaultValue;
	}

	/**
	 * Sets the default value of the field.
	 * @param defaultValue the parameter value
	 */
	public void setDefaultValue(Object defaultValue) {
		this.defaultValue = defaultValue;
	}

	/**
	 * get whether the field is nullable
	 * @return true if the field is optional
	 */
	public boolean isNullable() {
		return nullable;
	}

	/**
	 * Sets whether the field is nullable.
	 * @param nullable the parameter value
	 */
	public void setNullable(boolean nullable) {
		this.nullable = nullable;
	}

	@JsonIgnore
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
		this.sourcedatatype = sourcedatatype;
		return this;
	}

	/**
	 * Source specific data type string - used as information only
	 *
	 * @return the source data type identifier as specified
	 */
	@JsonGetter(COLUMN_PROP_SOURCEDATATYPE)
	public String getSourceDataType() {
		return sourcedatatype;
	}

	/**
	 * Define the security related sensitivity of this field.
	 *
	 * @param sensitivity of the field content
	 * @return this
	 */
	public AvroField setSensitivity(ContentSensitivity sensitivity) {
		this.sensitivity = sensitivity;
		return this;
	}

	/**
	 * get the field's content sensitivity
	 * 
	 * @return the field's content sensitivity
	 */
	@JsonGetter(COLUMN_PROP_CONTENT_SENSITIVITY)
	public ContentSensitivity getSensitivity() {
		return sensitivity;
	}

	/**
	 * The Avro field name has tight naming rules, less than what e.g. databases allow as field names
	 *
	 * @param name of the source field
	 */
	public void setOriginalName(String name) {
		this.originalname = name;
	}
	
	@JsonGetter(COLUMN_PROP_ORIGINALNAME)
	/**
	 * Get the original column name
	 * 
	 * @return the original column name
	 */
	public String getOriginalName() {
		return originalname;
	}

	/**
	 * Mark the field as an internal field, not one that is part of any official payload - source system id for example
	 *
	 * @param isinternal the parameter value
	 * @return this
	 */
	public AvroField setInternal(Boolean isinternal) {
		this.isinternal = isinternal;
		return this;
	}

	/**
	 * Is this field marked as internal?
	 *
	 * @return true is the field is marked as internal
	 */
	@JsonGetter(COLUMN_PROP_INTERNAL)
	public Boolean isInternal() {
		return isinternal;
	}

	/**
	 * Mark the field as technical field, a field that is not part of the actual payload but contains some more data needed for other reasons
	 *
	 * @param istechnical the parameter value
	 * @return this
	 */
	public AvroField setTechnical(Boolean istechnical) {
		this.istechnical = istechnical;
		return this;
	}

	/**
	 * Is this a technical field?
	 *
	 * @return true if the field was marked as technical field
	 */
	@JsonGetter(COLUMN_PROP_TECHNICAL)
	public Boolean getTechnical() {
		return istechnical;
	}

	/**
	 * 
	 * ColumnType tells something about the semantic usage of the field
	 */
	public enum ColumnType {
		/**
		 * a field that can be aggregated, e.g. a sales amount
		 */
		MEASURE,
		/**
		 * a field that describes an attribute, e.g. a product name
		 */
		ATTRIBUTE,
		/**
		 * a field that represents a currency
		 */
		CURRENCY,
		/**
		 * a field that represents a unit of measure
		 */
		UOM,
		/**
		 * a field that contains text data
		 */
		TEXT,
		/**
		 * a field that represents a hierarchy
		 */
		HIERARCHY
	}

	/**
	 * Sets the semantics of the field.
	 * @param semantics the parameter value
	 * @return this
	 */
	public AvroField setSemantics(ColumnSemantics semantics) {
		this.semantics = semantics;
		return this;
	}

	@JsonGetter(COLUMN_PROP_SEMANTICS)
	/**
	 * Gets the semantics of the field.
	 * @return the column semantics
	 */
	public ColumnSemantics getSemantics() {
		return this.semantics;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || !(o instanceof AvroField)) {
			return false;
		}
		AvroField other = (AvroField) o;
		if (!AvroUtils.isEqual(this.name, other.name)) {
			return false;
		}
		if (!AvroUtils.isEqual(this.doc, other.doc)) {
			return false;
		}
		if (!AvroUtils.isEqual(this.defaultValue, other.defaultValue)) {
			return false;
		}
		if (!AvroUtils.isEqual(this.nullable, other.nullable)) {
			return false;
		}
		if (!AvroUtils.isEqual(this.originalname, other.originalname)) {
			return false;
		}
		if (!AvroUtils.isEqual(this.sourcedatatype, other.sourcedatatype)) {
			return false;
		}
		if (!AvroUtils.isEqual(this.sensitivity, other.sensitivity)) {
			return false;
		}
		if (!AvroUtils.isEqual(this.isinternal, other.isinternal)) {
			return false;
		}
		if (!AvroUtils.isEqual(this.istechnical, other.istechnical)) {
			return false;
		}
		if (!AvroUtils.isEqual(this.semantics, other.semantics)) {
			return false;
		}
		if (!AvroUtils.isEqual(this.datatype, other.datatype)) {
			return false;
		}
		return true;
	}

	@Override
	public int hashCode() {
		return this.name.hashCode();
	}


}
