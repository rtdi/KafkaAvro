package io.rtdi.bigdata.kafka.avro.recordbuilders;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaBuilderException;
import org.apache.avro.generic.GenericRecord;

import io.rtdi.bigdata.kafka.avro.AvroNameEncoder;
import io.rtdi.bigdata.kafka.avro.AvroUtils;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroInt;

/**
 * The base class for creating key and value schemas and to create subschemas 
 *
 */
public class SchemaBuilder {

	private List<Field> columns = new ArrayList<>();
	private Map<String, AvroField> columnnameindex = new HashMap<>();
	private Schema schema;
	private boolean isbuilt = false;
	private Map<String, SchemaBuilder> childbuilders = new HashMap<>();

	/**
	 * Create a new schema with the given name, namespace is optionally provided as well
	 * 
	 * @param name of the schema
	 * @param namespace used for the schema or null
	 * @param description of the schema
	 */
	protected SchemaBuilder(String name, String namespace, String description) {
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
		schema = Schema.createRecord(AvroNameEncoder.encodeName(nameparts[nameparts.length-1]), description, ns.toString(), false);
		schema.addProp(AvroField.COLUMN_PROP_ORIGINALNAME, name);
	}
	
	/**
	 * Create a new schema with the given name, no extra namespace
	 * 
	 * @param name of the schema
	 * @param description of the schema
	 */
	public SchemaBuilder(String name, String description) {
		this(name, null, description);
	}
	
	/**
	 * Add a column of datatype record based on a provided SchemaBuilder.<br>
	 * Useful if an Avro Schema has multiple fields with the same Record datatype.
	 * 
	 * @param columnname of the field to add
	 * @param subschema SchemaBuilder used to form the child record schema
	 * @param description optional 
	 * @param nullable is true if the column can be null
	 * @return AvroRecordField of the column added 
	 * @throws SchemaBuilderException if the subschema is null
	 */
	public AvroRecordField addColumnRecord(String columnname, SchemaBuilder subschema, String description, boolean nullable) throws SchemaBuilderException {
		validate(columnname);
		if (subschema == null) {
			throw new SchemaBuilderException("Schema cannot be null or empty");
		}
		AvroRecordField field = new AvroRecordField(columnname, subschema, description, nullable, JsonProperties.NULL_VALUE);
		add(field);
		childbuilders.put(columnname, subschema);
		return field;
	}
	
	/**
	 * Add a column of type record and create a new schema.
	 * 
	 * @param columnname of the field to add
	 * @param description optional
	 * @param nullable is true if the column can be null
	 * @param schemaname of the schema to be created; if null the column name is used
	 * @param schemadescription of the schema to be created; if null the column description is used
	 * @return AvroRecordField 
	 * @throws SchemaBuilderException if the schema is invalid
	 * 
	 * @see AvroRecordField#getSchemaBuilder()
	 */
	public AvroRecordField addColumnRecord(String columnname, String description, boolean nullable, String schemaname, String schemadescription) throws SchemaBuilderException {
		SchemaBuilder subschema = createNewSchema((schemaname != null?schemaname:columnname), (schemadescription != null?schemadescription:description));
		return addColumnRecord(columnname, subschema, description, nullable);
	}

	/**
	 * Add a column of datatype array-of-scalar.
	 * 
	 * @param columnname of the array column
	 * @param arrayelement One of the Avroxxxx schemas like {@link AvroInt#getSchema()}
	 * @param description explaining the use of the field
	 * @return AvroArray, the added column
	 * @throws SchemaBuilderException if the schema is invalid
	 */
	public AvroArray addColumnArray(String columnname, Schema arrayelement, String description) throws SchemaBuilderException {
		validate(columnname);
		AvroArray field = new AvroArray(columnname, arrayelement, description, JsonProperties.NULL_VALUE);
		add(field);
		return field;
	}

	/**
	 * Add a column of datatype array-of-records based on an existing SchemaBuilder.
	 * 
	 * @param columnname of the array column
	 * @param arrayelement created via ValueSchema#createNewSchema(String, String)
	 * @param description explaining the use of the field
	 * @return AvroRecordArray, the added column
	 * @throws SchemaBuilderException if the schema is invalid
	 */
	public AvroRecordArray addColumnRecordArray(String columnname, SchemaBuilder arrayelement, String description) throws SchemaBuilderException {
		validate(columnname);
		
		AvroRecordArray field = new AvroRecordArray(columnname, arrayelement, description, JsonProperties.NULL_VALUE);
		add(field);
		childbuilders.put(columnname, arrayelement);
		return field;
	}

	/**
	 * @param columnname of the field to add
	 * @param description optional
	 * @param schemaname of the schema to be created; if null the column name is used
	 * @param schemadescription of the schema to be created; if null the column description is used
	 * @return AvroRecordArray
	 * @throws SchemaBuilderException if the schema is invalid
	 */
	public AvroRecordArray addColumnRecordArray(String columnname, String description, String schemaname, String schemadescription) throws SchemaBuilderException {
		SchemaBuilder subschema = createNewSchema((schemaname != null?schemaname:columnname), (schemadescription != null?schemadescription:description));
		return addColumnRecordArray(columnname, subschema, description);
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
	 * @see AvroField#AvroField(String, Schema, String, boolean, Object)
	 */
	public AvroField add(String columnname, Schema schema, String description, boolean nullable) throws SchemaBuilderException {
		validate(columnname, schema);
		AvroField field = new AvroField(columnname, schema, description, nullable, null); // JsonProperties.NULL_VALUE
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
	 * @see AvroField#AvroField(String, Schema, String, boolean, Object)
	 */
	public AvroField add(String columnname, Schema schema, String description, boolean nullable, Object defaultval) throws SchemaBuilderException {
		validate(columnname, schema);
		AvroField field = new AvroField(columnname, schema, description, nullable, defaultval); // JsonProperties.NULL_VALUE
		add(field);
		return field;
	}

	/**
	 * @param f field to be added
	 * @return this
	 * @throws SchemaBuilderException in case of errors
	 */
	public AvroField add(Field f) throws SchemaBuilderException {
		validate(f.name(), schema);
		AvroField field = new AvroField(f);
		add(field);
		return field;
	}
	
	/**
	 * @param columnname of the field
	 * @return the field object based on the column name
	 */
	public AvroField getField(String columnname) {
		return columnnameindex.get(columnname);
	}

	private void add(AvroField field) throws SchemaBuilderException {
		columns.add(field);
		columnnameindex.put(field.name(), field);
	}
	
	private void validate(String columnname) throws SchemaBuilderException {
		if (columnname == null || columnname.length() == 0) {
			throw new SchemaBuilderException("Columnname cannot be null or empty");
		}
		if (isbuilt) {
			throw new SchemaBuilderException("Record had been built already, cannot add more columns");
		}
	}
		
	private void validate(String columnname, Schema schema) throws SchemaBuilderException {
		validate(columnname);
		if (schema == null) {
			throw new SchemaBuilderException("Schema cannot be null or empty");
		}
	}
	
	/**
	 * Create a new schema based on the current schema name space
	 * 
	 * @param name of the new schema
	 * @param schemadescription with extra text
	 * @return a new schema builder
	 * @throws SchemaBuilderException if the schema was null
	 */
	protected SchemaBuilder createNewSchema(String name, String schemadescription) throws SchemaBuilderException {
		if (name == null || name.length() == 0) {
			throw new SchemaBuilderException("Schema name cannot be null or empty");
		}
		SchemaBuilder b = new SchemaBuilder(name, schema.getNamespace(), schemadescription);
		return b;
	}

	/**
	 * @param columnname to look for
	 * @return true in case the schema builder contains a column of that name already
	 */
	public boolean contains(String columnname) {
		return columnnameindex.containsKey(columnname);
	}

	/**
	 * @return the Avro schema as built
	 */
	public Schema getSchema() {
		return schema;
	}
	
	/**
	 * Once all columns are added to the schema it can be built and is locked then. 
	 * The build() process goes through all child record builders as well, building the entire schema.
	 * 
	 * @throws SchemaBuilderException if the schema has no columns
	 */
	public void build() throws SchemaBuilderException {
		if (columns.size() == 0) {
			throw new SchemaBuilderException("No schema definition found, schema builder " + schema.getName() + " has no columns");
		}
		if (!isbuilt) {
			isbuilt = true;
			schema.setFields(columns);
			for (SchemaBuilder child : childbuilders.values()) {
				child.build();
			}
		}
	}
	
	/**
	 * @return name of the schema
	 */
	public String getName() {
		return schema.getName();
	}
	
	/**
	 * @return the full name of the schema
	 */
	public String getFullName() {
		return schema.getFullName();
	}

	/**
	 * @return the schema description
	 */
	public String getDescription() {
		return schema.getDoc();
	}

	@Override
	public String toString() {
		return schema.getFullName();
	}
	
	/**
	 * @param columnname to look for (original name, not Avro encoded name)
	 * @return the Avro schema of this column
	 * @throws SchemaBuilderException if such column cannot be found
	 */
	public Schema getColumnSchema(String columnname) throws SchemaBuilderException {
		String encodedname = AvroNameEncoder.encodeName(columnname);
		Field f = columnnameindex.get(encodedname);
		if (f != null) {
			return f.schema();
		} else {
			throw new SchemaBuilderException("Requested field \"" + columnname + "\" (encoded: \"" + encodedname + "\") does not exist");
		}
	}


	/**
	 * Get the schema to use for all array items
	 * 
	 * @param valuerecord with the field
	 * @param fieldname to look for
	 * @return the array's expected schema for the items
	 * @throws SchemaBuilderException in case the field cannot be found or is not an array
	 */
	public static Schema getSchemaForArray(GenericRecord valuerecord, String fieldname) throws SchemaBuilderException {
		Field f = valuerecord.getSchema().getField(fieldname);
		if (f == null) {
			throw new SchemaBuilderException("The record and its supporting schema \"" + valuerecord.getSchema().getName() + 
					"\" does not have a field \"" + fieldname + "\"");
		}
		Schema s = AvroUtils.getBaseSchema(f.schema());
		if (s.getType() != Type.ARRAY) {
			throw new SchemaBuilderException("The record and its supporting schema \"" + valuerecord.getSchema().getName() + 
					"\" has a field \"" + fieldname + "\" but this is no array");
		}
		return s.getElementType();
	}

	/**
	 * Get the schema to use for the nested record
	 * 
	 * @param valuerecord with the field
	 * @param fieldname to look for
	 * @return the schema of this nested record
	 * @throws SchemaBuilderException in case the field cannot be found or is not a record
	 */
	public static Schema getSchemaForNestedRecord(GenericRecord valuerecord, String fieldname) throws SchemaBuilderException {
		Field f = valuerecord.getSchema().getField(fieldname);
		if (f == null) {
			throw new SchemaBuilderException("The record and its supporting schema \"" + valuerecord.getSchema().getName() + 
					"\" does not have a field \"" + fieldname + "\"");
		}
		Schema s = AvroUtils.getBaseSchema(f.schema());
		if (s.getType() != Type.RECORD) {
			throw new SchemaBuilderException("The record and its supporting schema \"" + valuerecord.getSchema().getName() + 
					"\" has a field \"" + fieldname + "\" but this is no nested record");
		}
		return s;
	}

}
