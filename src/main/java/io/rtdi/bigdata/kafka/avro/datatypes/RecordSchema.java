package io.rtdi.bigdata.kafka.avro.datatypes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaBuilderException;
import org.apache.avro.SchemaFormatter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData.Record;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import io.rtdi.bigdata.kafka.avro.AvroDataTypeException;
import io.rtdi.bigdata.kafka.avro.AvroNameEncoder;
import io.rtdi.bigdata.kafka.avro.AvroUtils;

/**
 * The base class for creating key and value schemas and to create subschemas
 *
 */
public class RecordSchema implements IAvroDatatype {

	private List<AvroField> fields = new ArrayList<>();
	private Map<String, AvroField> columnnameindex = new HashMap<>();
	/**
	 * Jackson om
	 */
	protected ObjectMapper om = AvroUtils.createJacksonOM();
	private String name;
	private String namespace;
	private String doc;
	private String orginalname;
	/**
	 * Schema type name
	 */
	public static final String NAME = "RECORD";

	/**
	 * Creates a new instance of this class.
	 */
	public RecordSchema() {
	}

    /**
     * get the type of this schema
	 * @return the type of this schema
     */
    public String getType() {
        return NAME;
    }

	/**
	 * set the type of this schema 
	 * @param name the parameter value
	 */
	public void setType(String name) {
		// ignore, this is only for Jackson to be able to deserialize the type
	}

	/**
	 * Create a new schema with the given name, namespace is optionally provided as well
	 *
	 * @param name of the schema
	 * @param namespace used for the schema or null
	 * @param doc description of the schema
	 */
	protected RecordSchema(String name, String namespace, String doc) {
		this.name = AvroNameEncoder.encodeName(name);
		this.namespace = namespace;
		this.doc = doc;
		this.orginalname = name;
	}

	/**
	 * Creates a new instance of this class.
	 * @param schema the parameter value
	 */
	public RecordSchema(Schema schema) {
		setName(schema.getName());
		setNamespace(schema.getNamespace());
		setDoc(schema.getDoc());
		String originalname = schema.getProp(AvroField.COLUMN_PROP_ORIGINALNAME);
		if (originalname != null) {
			setOrginalname(originalname);
		} else {
			setOrginalname(schema.getName());
		}
		List<Field> fields = schema.getFields();
		for (Field f : fields) {
			add(AvroField.create(f));
		}
	}

	/**
	 * get the namespace of the schema
	 * @return the schema's namespace
	 */
	public String getNamespace() {
		return this.namespace;
	}

	/**
	 * get the name of the schema
	 * @return the schema's name
	 */
	public String getName() {
		return this.name;
	}

	/**
	 * set the name of the schema
	 * @param name the parameter value
	 */
	public void setName(String name) {
		this.name = AvroUtils.nullif(name);
	}

	/**
	 * set the namespace of the schema
	 * @param namespace the parameter value
	 */
	public void setNamespace(String namespace) {
		this.namespace = AvroUtils.nullif(namespace);
	}

	/**
	 * set the documentation of the schema
	 * @param doc the parameter value
	 */
	public void setDoc(String doc) {
		this.doc = AvroUtils.nullif(doc);
	}

	/**
	 * get the documentation of the schema
	 * @return the schema's documentation
	 */
	public String getDoc() {
		return this.doc;
	}

	/**
	 * get the original name of the schema
	 * @return the schema's original name
	 */
	@JsonGetter(AvroField.COLUMN_PROP_ORIGINALNAME)
	public String getOrginalname() {
		if (this.orginalname == null) {
			return AvroNameEncoder.encodeName(this.name);
		}
		return this.orginalname;
	}

	/**
	 * set the original name of the schema
	 * @param orginalname the parameter value
	 */
	@JsonSetter(AvroField.COLUMN_PROP_ORIGINALNAME)
	public void setOrginalname(String orginalname) {
		this.orginalname = orginalname;
	}

	/**
	 * Create a new schema with the given name, no extra namespace
	 *
	 * @param name of the schema
	 * @param description of the schema
	 */
	public RecordSchema(String name, String description) {
		this();
		String[] nameparts = name.split("\\.");
		String[] namespaceparts = null;
		if (namespace != null) {
			namespaceparts = namespace.split("\\.");
		}
		StringBuffer ns = new StringBuffer();
		/*
		 * Add all namespace provided components and encode the names if needed
		 */
		if (namespaceparts != null) {
			for (String part : namespaceparts) {
				if (ns.length() != 0) {
					ns.append('.');
				}
				ns.append(AvroNameEncoder.encodeName(part));
			}
		}
		/*
		 * If the name contains namespace elements as well, add that to the namespace.
		 * Note the loop ends one before the last element!
		 */
		for (int i=0; i<nameparts.length-1; i++) {
			if (ns.length() != 0) {
				ns.append('.');
			}
			ns.append(AvroNameEncoder.encodeName(nameparts[i]));
		}
		/*
		 * Last element in the name is the name, all previous were namespaces
		 */
		this.name = AvroNameEncoder.encodeName(nameparts[nameparts.length-1]);
		if (ns.isEmpty()) {
			this.namespace = null;
		} else {
			this.namespace = ns.toString();
		}
		this.doc = description;
		this.orginalname = nameparts[nameparts.length-1];
	}


	/**
	 * Add columns to the current schema before it is built.<br>
	 * A typical call will look like
	 * <pre>add("col1", AvroNVarchar.getSchema(10), "first col", false);</pre>
	 *
	 * @param columnname of the field to add
	 * @param schema of the column; see io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes
	 * @param description of the column or null
	 * @param nullable is true if the column is optional
	 * @return AvroField to set other properties of the field (fluent syntax)
	 * @throws SchemaBuilderException if the schema is invalid
	 *
	 * @see AvroField#AvroField(String, IAvroDatatype, String, boolean, Object)
	 */
	public AvroField add(String columnname, IAvroDatatype schema, String description, boolean nullable) throws SchemaBuilderException {
		Object defaultval = null;
		AvroField field = new AvroField(columnname, schema, description, nullable, defaultval);
		add(field);
		return field;
	}

	/**
	 * Add columns to the current schema before it is built.<br>
	 * A typical call will look like
	 * <pre>add("col1", AvroNVarchar.getSchema(10), "first col", false);</pre>
	 *
	 * @param columnname of the field to add
	 * @param schema of the column; see io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes
	 * @param description of the column or null
	 * @param nullable is true if the column is optional
	 * @param defaultval default value
	 * @return AvroField to set other properties of the field (fluent syntax)
	 * @throws SchemaBuilderException if the schema is invalid
	 *
	 * @see AvroField#AvroField(String, IAvroDatatype, String, boolean, Object)
	 */
	public AvroField add(String columnname, IAvroDatatype schema, String description, boolean nullable, Object defaultval) throws SchemaBuilderException {
		AvroField field = new AvroField(columnname, schema, description, nullable, defaultval);
		add(field);
		return field;
	}

	/**
	 * get the field of the schema by name, null if not found
	 * @param columnname of the field
	 * @return the field object based on the column name
	 */
	@JsonIgnore
	public AvroField getField(String columnname) {
		return columnnameindex.get(columnname);
	}

	/**
	 * add a field to the schema and update the columnname index
	 * @param field to add
	 * @throws SchemaBuilderException in case of an error
	 */
	protected void add(AvroField field) throws SchemaBuilderException {
		fields.add(field);
		columnnameindex.put(field.getName(), field);
	}


	/**
	 * Check if the schema builder contains a column of that name already
	 * @param columnname to look for
	 * @return true in case the schema builder contains a column of that name already
	 */
	public boolean contains(String columnname) {
		return columnnameindex.containsKey(columnname);
	}

	/**
	 * Create the Avro schema
	 * @return the Avro schema as built
	 */
	public Schema createSchema() {
		List<Schema.Field> fields = new ArrayList<>();
		for (AvroField f : this.fields) {
			fields.add(f.getAvroField());
		}
		Schema schema = Schema.createRecord(this.name, this.doc, this.namespace, false, fields);
		schema.addProp(AvroField.COLUMN_PROP_ORIGINALNAME, this.orginalname);
		return schema;
	}

	/**
	 * Get all fields of the schema
	 * @return the schema's fields
	 */
	public List<AvroField> getFields() {
		return this.fields;
	}

	/**
	 * set the fields of the schema
	 * @param fields the parameter value
	 */
	public void setFields(List<AvroField> fields) {
		this.fields = fields;
		this.columnnameindex.clear();
		for (AvroField f : fields) {
			this.columnnameindex.put(f.getName(), f);
		}
	}


	/**
	 * Get the full name of the schema, which is namespace.name
	 * @return the full name of the schema
	 */
	@JsonIgnore
	public String getFullName() {
		if (this.namespace == null || this.namespace.length() == 0) {
			return this.name;
		} else {
			return this.namespace + "." + this.name;
		}
	}


	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || !(o instanceof RecordSchema)) {
			return false;
		}
		RecordSchema other = (RecordSchema) o;
		return attributesEqual(other) && fieldsEqual(other);
	}

	private boolean fieldsEqual(RecordSchema other) {
		for(AvroField f : this.fields) {
			if (!f.equals(other.getField(f.getName()))) {
				return false;
			}
		}
		return true;
	}

	private boolean attributesEqual(RecordSchema other) {
		if (!AvroUtils.isEqual(this.name, other.name)) {
			return false;
		}
		if (!AvroUtils.isEqual(this.namespace, other.namespace)) {
			return false;
		}
		if (!AvroUtils.isEqual(this.doc, other.doc)) {
			return false;
		}
		if (!AvroUtils.isEqual(this.orginalname, other.orginalname)) {
			return false;
		}
		return true;
	}

	@Override
	public int hashCode() {
		return this.createSchema().hashCode();
	}

	@Override
	public String toString() {
		return this.getFullName();
	}

	@Override
	public void toString(StringBuffer b, Object value) {
		if (value instanceof Record) {
			Record r = (Record) value;
			Schema schema = r.getSchema();
			b.append('{');
			boolean first = true;
			for (Field f : schema.getFields()) {
				Object v = r.get(f.pos());
				if (v != null) {
					IAvroDatatype datatype = AvroType.getAvroDataType(f.schema());
					if (datatype != null) {
						if (!first) {
							b.append(',');
						} else {
							first = false;
						}
						b.append('\"');
						b.append(f.name());
						b.append("\":");
						datatype.toString(b, v);
					}
				}
			}
			b.append('}');
		}
	}

	@Override
	public GenericRecord convertToInternal(Object value) throws AvroDataTypeException {
		if (value == null) {
			return null;
		} else if (value instanceof GenericRecord) {
			return (GenericRecord) value;
		}
		throw new AvroDataTypeException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a GenericRecord");
	}

	@Override
	public GenericRecord convertToJava(Object value) throws AvroDataTypeException {
		if (value == null) {
			return null;
		} else if (value instanceof GenericRecord) {
			return (GenericRecord) value;
		}
		throw new AvroDataTypeException("Cannot convert a value of type \"" + value.getClass().getSimpleName() + "\" into a GenericRecord");
	}

	@Override
	public Type getBackingType() {
		return Type.RECORD;
	}

	@Override
	public Schema getDatatypeSchema() {
		return this.createSchema();
	}

	@Override
	public AvroType getAvroType() {
		return AvroType.AVRORECORD;
	}

	@Override
	public String convertToJson(Object value) throws AvroDataTypeException, JsonProcessingException {
		GenericRecord b = convertToJava(value);
		if (b == null) {
			return "null";
		} else {
			StringBuffer sb = new StringBuffer("{");
			for (Field f : b.getSchema().getFields()) {
				Object v = b.get(f.pos());
				IAvroDatatype datatype = AvroType.getAvroDataType(AvroUtils.getBaseSchema(f.schema()));
				if (sb.length() > 1) {
					sb.append(',');
				}
				sb.append('\"');
				sb.append(f.name());
				sb.append("\":");
				sb.append(datatype.convertToJson(v));
			}
			sb.append("}");
			return sb.toString();
		}
	}

	/**
	 * Create the Avro schema and convert it into Json..
	 * Returns the official Avro schema.
	 * @return the Avro schema in JSON format
	 */
	public String toAvroJson() {
		return SchemaFormatter.format("json/pretty", this.createSchema());
	}

	/**
	 * Serialize the class into object json.
	 * @return the serialized class in JSON format
	 * @throws JsonProcessingException in case the serialization fails
	 */
	public String toObjectJson() throws JsonProcessingException {
		return om.writeValueAsString(this);
	}

	/**
	 * Deserialize the class from object json.
	 * @param json the parameter value
	 * @return the resulting value
	 * @throws JsonMappingException in case the deserialization fails
	 * @throws JsonProcessingException in case the deserialization fails
	 */
	public static RecordSchema fromObjectJson(String json) throws JsonMappingException, JsonProcessingException {
		ObjectMapper om = new ObjectMapper().disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
		return om.readValue(json, RecordSchema.class);
	}

}
