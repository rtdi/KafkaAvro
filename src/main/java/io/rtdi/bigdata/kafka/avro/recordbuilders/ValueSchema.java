package io.rtdi.bigdata.kafka.avro.recordbuilders;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilderException;

import io.rtdi.bigdata.kafka.avro.AvroUtils;
import io.rtdi.bigdata.kafka.avro.RowType;
import io.rtdi.bigdata.kafka.avro.SchemaConstants;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroAnyPrimitive;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroByte;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroMap;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroNVarchar;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroString;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroTimestamp;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroVarchar;

/**
 * A class that helps creating an AvroSchema by code for the value record.
 * It is a custom built Avro schema plus extra columns.
 *
 */
public class ValueSchema extends SchemaBuilder {

	public static final String AUDIT_TRANSFORMRESULT_QUALITY = "__transformresult_quality";
	public static final String AUDITTRANSFORMRESULTTEXT = "__transformresult_text";
	public static final String AUDITTRANSFORMATIONNAME = "__transformationname";
	public static final String AUDITDETAILS = "__details";
	public static final String TRANSFORMRESULT = "__transformresult";
	public static final String AUDIT = "__audit";
	private static final String SCHEMA_INFO_REGULATIONS = "regulations";
	public static final String SCHEMA_INFO_DATAPRODUCT_OWNER = "data_product_owner_email";
	public static final String PRIMARY_KEYS = "pks";
	public static final String FOREIGN_KEYS = "fks";
	public static SchemaBuilder extension;
	public static SchemaBuilder audit;
	public static AvroRecordArray audit_details;
	public static Schema auditdetails_array_schema;

	static {
		try {
			extension = new SchemaBuilder("__extension", "Extension point to add custom values to each record");
			extension.add("__path", AvroString.getSchema(), "An unique identifier, e.g. \"street\".\"house number component\"", false);
			extension.add("__value", AvroAnyPrimitive.getSchema(), "The value of any primitive datatype of Avro", false);
			extension.build();

			audit = new SchemaBuilder(AUDIT, "If data is transformed this information is recorded here");
			audit.add(TRANSFORMRESULT, AvroVarchar.getSchema(4), "Is the record PASS, FAILED or WARN?", false);
			audit_details = audit.addColumnRecordArray(AUDITDETAILS, "Details of all transformations", "__audit_details", null);
			audit_details.add(AUDITTRANSFORMATIONNAME, AvroNVarchar.getSchema(1024), "A name identifying the applied transformation", false);
			audit_details.add(TRANSFORMRESULT, AvroVarchar.getSchema(4), "Is the record PASS, FAIL or WARN?", false);
			audit_details.add(AUDITTRANSFORMRESULTTEXT, AvroNVarchar.getSchema(1024), "Transforms can optionally describe what they did", true);
			audit_details.add(AUDIT_TRANSFORMRESULT_QUALITY, AvroByte.getSchema(), "Transforms can optionally return a percent value from 0 (FAIL) to 100 (PASS)", true);
			audit.build();
			auditdetails_array_schema = AvroUtils.getBaseSchema(audit_details.schema());
			auditdetails_array_schema.getElementType();
		} catch (SchemaBuilderException e) {
			e.printStackTrace();
		}
	}

	/**
	 * In order to create a complex Avro schema for the value record from scratch, this builder is used.<br>
	 * It adds mandatory columns to the root level and optional extension columns.
	 *
	 * @param name of the schema
	 * @param namespace optional, to make sure two schemas with the same name but different meanings can be separated
	 * @param description optional text
	 * @throws SchemaBuilderException if the schema is invalid
	 */
	public ValueSchema(String name, String namespace, String description) throws SchemaBuilderException {
		super(name, namespace, description);
		add(SchemaConstants.SCHEMA_COLUMN_CHANGE_TYPE,
				AvroVarchar.getSchema(1),
				"Indicates how the row is to be processed: Insert, Update, Delete, upsert/Autocorrect, eXterminate, Truncate",
				false, RowType.UPSERT.name()).setInternal().setTechnical();
		add(SchemaConstants.SCHEMA_COLUMN_TRUNCATE,
				AvroMap.getSchema(AvroString.getSchema()),
				"In case of a change type of TRUNCATE, this map contains the fields to identify the set of rows to be deleted",
				false);
		add(SchemaConstants.SCHEMA_COLUMN_CHANGE_TIME,
				AvroTimestamp.getSchema(),
				"Timestamp of the transaction. All rows of the transaction have the same value.",
				false, 0).setInternal().setTechnical();
		add(SchemaConstants.SCHEMA_COLUMN_SOURCE_ROWID,
				AvroVarchar.getSchema(30),
				"Optional unqiue and static pointer to the row, e.g. Oracle rowid",
				true).setInternal().setTechnical();
		add(SchemaConstants.SCHEMA_COLUMN_SOURCE_TRANSACTION,
				AvroVarchar.getSchema(30),
				"Optional source transaction information for auditing",
				true).setInternal().setTechnical();
		add(SchemaConstants.SCHEMA_COLUMN_SOURCE_SYSTEM,
				AvroVarchar.getSchema(30),
				"Optional source system information for auditing",
				true).setInternal().setTechnical();
		addColumnArray(SchemaConstants.SCHEMA_COLUMN_EXTENSION, extension.getSchema(), "Add more columns beyond the official logical data model").setInternal();
		addAuditField(this);
	}

	/**
	 * Add regulations that apply to this schema, e.g. GDPR, HIPAA, CCPA, ...
	 *
	 * @param regulations list of regulations
	 */
	public void setRegulations(Collection<String> regulations) {
		addProp(SCHEMA_INFO_REGULATIONS, regulations);
	}

	/**
	 * @return the regulations that apply to this schema, e.g. GDPR, HIPAA, CCPA, ...
	 */
	@SuppressWarnings("unchecked")
	public Collection<String> getRegulations() {
		return getProp(SCHEMA_INFO_REGULATIONS, Collection.class);
	}

	/**
	 * Set the email address of the data product owner
	 *
	 * @param email as string - not validated
	 */
	public void setDataProductOwner(String email) {
		addProp(SCHEMA_INFO_DATAPRODUCT_OWNER, email);
	}

	/**
	 * Get the email address of the data product owner
	 *
	 * @return email as string - not validated
	 */
	public String getDataProductOwner() {
		return getProp(SCHEMA_INFO_DATAPRODUCT_OWNER, String.class);
	}

	/**
	 * Set the primary key columns of this schema.
	 *
	 * @param columnnames to be used in the root schema as primary key
	 */
	public void setPrimaryKey(String... columnnames) {
		addProp(PRIMARY_KEYS, Arrays.asList(columnnames));
	}

	/**
	 * Add a foreign key relationship to another schema.
	 *
	 * @param name
	 * @param condition
	 */
	public void addForeignKey(String name, FKCondition condition) {
		List<FKCondition> fks = getForeignKeys();
		if (fks == null) {
			fks = new ArrayList<>();
			addProp(FOREIGN_KEYS, fks);
		}
		fks.add(condition);
	}

	/**
	 * Get all FK relationships from this schema to other schemas.
	 *
	 * @return list of FKConditions or null if none are defined
	 */
	@SuppressWarnings("unchecked")
	public List<FKCondition> getForeignKeys() {
		return getProp(FOREIGN_KEYS, List.class);
	}

	/**
	 * @return the list of all primary key columns of the root schema
	 */
	@SuppressWarnings("unchecked")
	public List<String> getPrimaryKeys() {
		return getProp(PRIMARY_KEYS, List.class);
	}

	/**
	 * @param name of the value schema
	 * @param description free for text
	 * @throws SchemaBuilderException if the schema is invalid
	 * @see #ValueSchema(String, String, String)
	 */
	public ValueSchema(String name, String description) throws SchemaBuilderException {
		this(name, null, description);
	}

	public static void addAuditField(SchemaBuilder builder) {
		builder.addColumnRecord(AUDIT, audit, "If data is transformed this information is recorded here", true).setInternal();
	}

	/**
	 * While a normal child schema has just the added columns, a child schema of the ValueSchema has an additional extension column always.
	 *
	 * @see SchemaBuilder#createNewSchema(String, String)
	 */
	@Override
	protected SchemaBuilder createNewSchema(String name, String schemadescription) throws SchemaBuilderException {
		SchemaBuilder child = super.createNewSchema(name, schemadescription);
		child.addColumnArray(SchemaConstants.SCHEMA_COLUMN_EXTENSION, extension.getSchema(), "Add more columns beyond the official logical data model");
		return child;
	}

}
