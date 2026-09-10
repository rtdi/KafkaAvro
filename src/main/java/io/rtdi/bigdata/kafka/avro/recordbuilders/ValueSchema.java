package io.rtdi.bigdata.kafka.avro.recordbuilders;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalTypes.BigDecimal;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilderException;
import org.apache.avro.SchemaFormatter;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.rtdi.bigdata.kafka.avro.AvroUtils;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroArray;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroByte;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroMap;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroNVarchar;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroString;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroTimestamp;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroType;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroVarchar;
import io.rtdi.bigdata.kafka.avro.datatypes.RecordSchema;
import io.rtdi.bigdata.kafka.avro.objects.DeletionPolicy;
import io.rtdi.bigdata.kafka.avro.objects.Duration;
import io.rtdi.bigdata.kafka.avro.objects.FKCondition;
import io.rtdi.bigdata.kafka.avro.objects.RLS;
import io.rtdi.bigdata.kafka.avro.objects.TableSemantics;

/**
 * A class that helps creating an AvroSchema by code for the value record.
 * It is a custom built Avro schema plus extra columns.
 *
 */
public class ValueSchema extends RecordSchema {

	/**
	 * Schema property name for regulations that apply to this schema
	 */
	public static final String SCHEMA_INFO_REGULATIONS = "data_classifications";
	/**
	 * Schema property name for retention period
	 */
	public static final String SCHEMA_INFO_RETENTION_PERIOD = "retention_period";
	/**
	 * Schema property name for deletion policy
	 */
	public static final String SCHEMA_INFO_DELETION_POLICY = "deletion_policy";
	/**
	 * Schema property name for the data product owner email address
	 */
	public static final String SCHEMA_INFO_DATAPRODUCT_OWNER = "data_product_owner_email";
	/**
	 * Schema property name for the ticket system url
	 */
	public static final String SCHEMA_INFO_TICKETS_URL = "tickets_url";
	/**
	 * Schema property name for the repository url
	 */
	public static final String SCHEMA_INFO_REPO_URL = "repo_url";

	/**
	 * Schema property name for the object level security field
	 */
	public static final String SCHEMA_INFO_OBJECT_LEVEL_SECURITY = "object_level_security";
	/**
	 * Schema property name for the row level security field
	 */
	public static final String SCHEMA_INFO_ROW_LEVEL_SECURITY = "row_level_security";
	/**
	 * Schema property name for the additional partition-by fields
	 */
	public static final String SCHEMA_INFO_PARTITION_BY = "partition_by";
	/**
	 * Schema property name for the semantics
	 */
	public static final String SCHEMA_INFO_SEMANTICS = "semantics";
	/**
	 * Schema property name for the primary key column names (list of strings)
	 */
	public static final String PRIMARY_KEYS = "pks";
	/**
	 * Schema property name for the foreign key relationships (list of FKCondition objects)
	 */
	public static final String FOREIGN_KEYS = "fks";

	private List<FKCondition> fks;
	private List<RLS> row_level_security;
	private Collection<String> regulations;
	private String ticketurl;
	private String repourl;
	private List<String> objectlevelsecurity;
	private List<String> partition_by;
	private TableSemantics semantics;
	private String dataproductowner;
	private List<String> pks;
	private Duration retentionperiod;
	private DeletionPolicy deletionpolicy;
	private final ObjectMapper om = new ObjectMapper();


	/**
	 * In order to create a complex Avro schema for the value record from scratch, this builder is used.<br>
	 * It adds mandatory columns to the root level and optional extension columns.
	 *
	 * @param name of the schema
	 * @param namespace optional, to make sure two schemas with the same name but different meanings can be separated
	 * @param description optional text
	 * @throws SchemaBuilderException if the schema is invalid
	 */
	/**
	 * Creates a new value schema builder with the given schema name, namespace, and description.
	 *
	 * @param name the schema name
	 * @param namespace the optional namespace used to disambiguate the schema
	 * @param description the optional schema description
	 * @throws SchemaBuilderException if the schema cannot be constructed
	 */
	public ValueSchema(String name, String namespace, String description) throws SchemaBuilderException {
		super(name, namespace, description);
		add(SchemaConstants.SCHEMA_COLUMN_CHANGE_TYPE,
				AvroVarchar.create(1),
				"Indicates how the row is to be processed: Insert, Update, Delete, upsert/Autocorrect, eXterminate, Truncate,...",
				false, RowType.UPSERT.name()).setInternal(true).setTechnical(true);
		add(SchemaConstants.SCHEMA_COLUMN_TRUNCATE,
				new AvroMap(AvroString.create()),
				"In case of a change type of TRUNCATE, this map contains the fields to identify the set of rows to be deleted",
				false);
		add(SchemaConstants.SCHEMA_COLUMN_CHANGE_TIME,
				AvroTimestamp.create(),
				"Timestamp of the transaction. All rows of the transaction have the same value.",
				false, 0).setInternal(true).setTechnical(true);
		add(SchemaConstants.SCHEMA_COLUMN_SOURCE_ROWID,
				AvroVarchar.create(30),
				"Optional unqiue and static pointer to the row, e.g. Oracle rowid",
				true).setInternal(true).setTechnical(true);
		add(SchemaConstants.SCHEMA_COLUMN_SOURCE_TRANSACTION,
				AvroVarchar.create(30),
				"Optional source transaction information for auditing",
				true).setInternal(true).setTechnical(true);
		add(SchemaConstants.SCHEMA_COLUMN_SOURCE_SYSTEM,
				AvroVarchar.create(30),
				"Optional source system information for auditing",
				true).setInternal(true).setTechnical(true);
        add(SchemaConstants.SCHEMA_COLUMN_EXTENSION_MAP, new AvroMap(AvroString.create()), "Add more values as a map beyond the official logical data model", true).setInternal(true);

		RecordSchema audit = new RecordSchema(SchemaConstants.AUDIT, "If data is transformed this information is recorded here");
		audit.add(SchemaConstants.TRANSFORMRESULT, AvroVarchar.create(4), "Is the record PASS, FAILED or WARN?", false);
		RecordSchema audit_details = new RecordSchema("__audit_details", "Details of all transformations");
		audit_details.add(SchemaConstants.AUDITTRANSFORMATIONNAME, AvroNVarchar.create(1024), "A name identifying the applied transformation", false);
		audit_details.add(SchemaConstants.TRANSFORMRESULT, AvroVarchar.create(4), "Is the record PASS, FAIL or WARN?", false);
		audit_details.add(SchemaConstants.AUDITTRANSFORMRESULTTEXT, AvroNVarchar.create(1024), "Transforms can optionally describe what they did", true);
		audit_details.add(SchemaConstants.AUDIT_TRANSFORMRESULT_QUALITY, AvroByte.create(), "Transforms can optionally return a percent value from 0 (FAIL) to 100 (PASS)", true);
		audit.add(SchemaConstants.AUDITDETAILS, new AvroArray(audit_details), "Details of all transformations", true);

		add(SchemaConstants.AUDIT, audit, "If data is transformed, this information is recorded here", true).setInternal(true);
	}

	/**
	 * Creates a new value schema builder with the given schema name and description.
	 *
	 * @param name the schema name
	 * @param description the optional schema description
	 * @throws SchemaBuilderException if the schema cannot be constructed
	 * @see #ValueSchema(String, String, String)
	 */
	public ValueSchema(String name, String description) throws SchemaBuilderException {
		this(name, null, description);
	}

	/**
	 * Creates an empty value schema builder instance.
	 */
	public ValueSchema() {
		super();
	}

	/**
	 * Creates a value schema builder from an existing Avro schema definition.
	 *
	 * @param schema the source Avro schema
	 */
	public ValueSchema(Schema schema) {
		super(schema);
		setPrimaryKeys(AvroType.getStringListProp(schema, PRIMARY_KEYS));
		setForeignKeys(AvroType.getTypedListProp(schema, FOREIGN_KEYS, FKCondition.class));
		setRegulations(AvroType.getStringListProp(schema, SCHEMA_INFO_REGULATIONS));
		setRetentionPeriod(AvroType.getProp(schema, SCHEMA_INFO_RETENTION_PERIOD, Duration.class));
		setDeletionPolicy(AvroType.getProp(schema, SCHEMA_INFO_DELETION_POLICY, DeletionPolicy.class));
		setDataProductOwner(AvroType.getProp(schema, SCHEMA_INFO_DATAPRODUCT_OWNER, String.class));
		setTicketUrl(AvroType.getProp(schema, SCHEMA_INFO_TICKETS_URL, String.class));
		setRepoUrl(AvroType.getProp(schema, SCHEMA_INFO_REPO_URL, String.class));
		setObjectLevelSecurity(AvroType.getStringListProp(schema, SCHEMA_INFO_OBJECT_LEVEL_SECURITY));
		setRowLevelSecurity(AvroType.getTypedListProp(schema, SCHEMA_INFO_ROW_LEVEL_SECURITY, RLS.class));
		setPartitionBy(AvroType.getStringListProp(schema, SCHEMA_INFO_PARTITION_BY));
		setSemantics(AvroType.getProp(schema, SCHEMA_INFO_SEMANTICS, TableSemantics.class));
	}

	/**
	 * Sets the list of regulations that apply to the schema.
	 *
	 * @param regulations the applicable regulation codes
	 */
	public void setRegulations(Collection<String> regulations) {
		this.regulations = regulations;
	}

	/**
	 * Sets the list of regulations that apply to the schema using a varargs list.
	 *
	 * @param regulations the applicable regulation codes
	 */
	public void setRegulations(String... regulations) {
		this.regulations = Arrays.asList(regulations);
	}

	/**
	 * Gets the schema-level regulation list.
	 *
	 * @return the regulation codes that apply to the schema
	 */
	public Collection<String> getRegulations() {
		return regulations;
	}

	/**
	 * Sets the ticket-tracking URL for the schema.
	 *
	 * @param url the ticket system URL
	 */
	public void setTicketUrl(String url) {
		this.ticketurl = url;
	}

	/**
	 * Gets the ticket-tracking URL for the schema.
	 *
	 * @return the ticket system URL
	 */
	public String getTicketUrl() {
		return ticketurl;
	}

	/**
	 * Sets the repository URL for the schema’s data product.
	 *
	 * @param url the repository URL
	 */
	public void setRepoUrl(String url) {
		this.repourl = url;
	}

	/**
	 * Gets the repository URL for the schema’s data product.
	 *
	 * @return the repository URL
	 */
	public String getRepoUrl() {
		return repourl;
	}


	/**
	 * Sets the object-level security entries for the schema.
	 *
	 * @param object_level_security the object-level security entries
	 */
	public void setObjectLevelSecurity(List<String> object_level_security) {
		this.objectlevelsecurity = object_level_security;
	}

	/**
	 * Sets the object-level security entries for the schema using a varargs list.
	 *
	 * @param object_level_security the object-level security entries
	 */
	public void setObjectLevelSecurity(String... object_level_security) {
		this.objectlevelsecurity = Arrays.asList(object_level_security);
	}

	/**
	 * Gets the object-level security entries for the schema.
	 *
	 * @return the object-level security entries
	 */
	public List<String> getObjectLevelSecurity() {
		return this.objectlevelsecurity;
	}

	/**
	 * Adds a row-level security rule for a dimension and field.
	 *
	 * @param dimension the dimension name used in the permission table
	 * @param field the field in the schema that carries the dimension value
	 */
	public void addRowLevelSecurity(String dimension, String field) {
		if (row_level_security == null) {
			row_level_security = new ArrayList<>();
		}
		row_level_security.add(new RLS(dimension, field));
	}

	/**
	 * Sets the row-level security rules for the schema.
	 *
	 * @param row_level_security the row-level security rules
	 */
	public void setRowLevelSecurity(List<RLS> row_level_security) {
		this.row_level_security = row_level_security;
	}

	/**
	 * Gets the row-level security rules for the schema.
	 *
	 * @return the row-level security rules
	 */
	public List<RLS> getRowLevelSecurity() {
		return this.row_level_security;
	}

	/**
	 * Sets the partitioning columns for the schema.
	 *
	 * @param partition_by the partition-by column names
	 */
	public void setPartitionBy(List<String> partition_by) {
		this.partition_by = partition_by;
	}

	/**
	 * Gets the partitioning columns for the schema.
	 *
	 * @return the partition-by column names
	 */
	public List<String> getPartitionBy() {
		return partition_by;
	}

	/**
	 * Sets the schema semantics metadata.
	 *
	 * @param semantics the semantic definition for the schema
	 */
	public void setSemantics(TableSemantics semantics) {
		this.semantics = semantics;
	}
	
	/**
	 * Gets the schema semantics metadata.
	 *
	 * @return the semantic definition for the schema
	 */
	public TableSemantics getSemantics() {
		return this.semantics;
	}

	/**
	 * Sets the email address of the data product owner.
	 *
	 * @param email the owner email address
	 */
	public void setDataProductOwner(String email) {
		this.dataproductowner = email;
	}

	/**
	 * Gets the email address of the data product owner.
	 *
	 * @return the owner email address
	 */
	public String getDataProductOwner() {
		return this.dataproductowner;
	}


	/**
	 * Sets the primary key columns for the schema using a varargs list.
	 *
	 * @param columnnames the primary key column names
	 */
	public void setPrimaryKeys(String... columnnames) {
		this.pks = Arrays.asList(columnnames);
	}

	/**
	 * Sets the primary key columns for the schema using a list.
	 *
	 * @param columnnames the primary key column names
	 */
	public void setPrimaryKeys(List<String> columnnames) {
		this.pks = columnnames;
	}

	/**
	 * Gets the primary key columns for the schema.
	 *
	 * @return the primary key column names
	 */
	public List<String> getPrimaryKeys() {
		return this.pks;
	}

	/**
	 * Adds a foreign key relationship to the schema.
	 *
	 * @param condition the foreign key condition to add
	 */
	public void addForeignKey(FKCondition condition) {
		if (fks == null) {
			fks = new ArrayList<>();
		}
		fks.add(condition);
	}

	/**
	 * Replaces the foreign key relationship list for the schema.
	 *
	 * @param fks the foreign key conditions
	 */
	public void setForeignKeys(List<FKCondition> fks) {
		this.fks = fks;
	}

	/**
	 * Gets the foreign key relationships defined for the schema.
	 *
	 * @return the foreign key conditions
	 */
	public List<FKCondition> getForeignKeys() {
		return this.fks;
	}

	/**
	 * Adds a foreign key relationship using the explicit relationship fields.
	 *
	 * @param name the foreign-key name
	 * @param schema_fqn the fully qualified target schema name
	 * @param left the left-side column name
	 * @param right the right-side column name
	 * @param condition an optional join condition
	 */
	public void addForeignKey(String name, String schema_fqn, String left, String right, String condition) {
		FKCondition fk = new FKCondition(name, schema_fqn, left, right, condition);
		addForeignKey(fk);
	}

	/**
	 * Sets the retention period hint for the schema.
	 *
	 * @param period the retention duration
	 */
	public void setRetentionPeriod(Duration period) {
		this.retentionperiod = period;
	}

	/**
	 * Gets the retention period hint for the schema.
	 *
	 * @return the retention duration, or {@code null} if not set
	 */
	public Duration getRetentionPeriod() {
		return this.retentionperiod;
	}

	/**
	 * Sets the deletion policy hint for the schema.
	 *
	 * @param policy the deletion policy
	 */
	public void setDeletionPolicy(DeletionPolicy policy) {
		this.deletionpolicy = policy;
	}

	/**
	 * Gets the deletion policy hint for the schema.
	 *
	 * @return the deletion policy, or {@code null} if not set
	 */
	public DeletionPolicy getDeletionPolicy() {
		return this.deletionpolicy;
	}

	/**
	 * Creates an Avro schema definition from the current builder state.
	 *
	 * @return the generated schema
	 */
	@Override
	public Schema createSchema() {
		Schema s = super.createSchema();
		addProp(s, PRIMARY_KEYS, this.pks);
		addProp(s, FOREIGN_KEYS, this.fks);
		addProp(s, SCHEMA_INFO_REGULATIONS, this.regulations);
		addProp(s, SCHEMA_INFO_RETENTION_PERIOD, this.retentionperiod);
		addProp(s, SCHEMA_INFO_DELETION_POLICY, this.deletionpolicy);
		addProp(s, SCHEMA_INFO_DATAPRODUCT_OWNER, this.dataproductowner);
		addProp(s, SCHEMA_INFO_TICKETS_URL, this.ticketurl);
		addProp(s, SCHEMA_INFO_REPO_URL, this.repourl);
		addProp(s, SCHEMA_INFO_OBJECT_LEVEL_SECURITY, this.objectlevelsecurity);
		addProp(s, SCHEMA_INFO_ROW_LEVEL_SECURITY, this.row_level_security);
		addProp(s, SCHEMA_INFO_PARTITION_BY, this.partition_by);
		addProp(s, SCHEMA_INFO_SEMANTICS, this.semantics);
		return s;
	}

	private void addProp(Schema s, String name, Object value) {
		if (value != null) {
			if (value == JsonProperties.NULL_VALUE) {
				s.addProp(name, value);
			} else if (value instanceof Map) { // record, map
				s.addProp(name, om.valueToTree(value));
			} else if (value instanceof Collection) { // array
				s.addProp(name, om.valueToTree(value));
			} else if (value instanceof byte[]) { // bytes, fixed
				s.addProp(name, value);
			} else if (value instanceof CharSequence || value instanceof Enum<?>) { // string, enum
				s.addProp(name, value);
			} else if (value instanceof Double) { // double
				s.addProp(name, value);
			} else if (value instanceof Float) { // float
				s.addProp(name, value);
			} else if (value instanceof Long) { // long
				s.addProp(name, value);
			} else if (value instanceof Integer) { // int
				s.addProp(name, value);
			} else if (value instanceof Boolean) { // boolean
				s.addProp(name, value);
			} else if (value instanceof BigInteger) {
				s.addProp(name, value);
			} else if (value instanceof BigDecimal) {
				s.addProp(name, value);
			} else {
				s.addProp(name, om.valueToTree(value));
			}
		}
	}

	/**
	 * Formats the current schema as pretty-printed Avro JSON.
	 *
	 * @return the schema formatted as Avro JSON
	 */
	public String toAvroJson() {
		return SchemaFormatter.format("json/pretty", this.createSchema());
	}

	/**
	 * Compares this value schema with another object for equality.
	 *
	 * @param o the object to compare against
	 * @return {@code true} when the two objects represent the same schema state
	 */
	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || !(o instanceof ValueSchema)) {
			return false;
		}
		ValueSchema other = (ValueSchema) o;
		if (!super.equals(other)) {
			return false;
		}
		if (!AvroUtils.isEqual(this.fks, other.fks)) {
			return false;
		}
		if (!AvroUtils.isEqual(this.row_level_security, other.row_level_security)) {
			return false;
		}
		if (!AvroUtils.isEqual(this.regulations, other.regulations)) {
			return false;
		}
		if (!AvroUtils.isEqual(this.ticketurl, other.ticketurl)) {
			return false;
		}
		if (!AvroUtils.isEqual(this.repourl, other.repourl)) {
			return false;
		}
		if (!AvroUtils.isEqual(this.objectlevelsecurity, other.objectlevelsecurity)) {
			return false;
		}
		if (!AvroUtils.isEqual(this.partition_by, other.partition_by)) {
			return false;
		}
		if (!AvroUtils.isEqual(this.semantics, other.semantics)) {
			return false;
		}
		if (!AvroUtils.isEqual(this.dataproductowner, other.dataproductowner)) {
			return false;
		}
		if (!AvroUtils.isEqual(this.pks, other.pks)) {
			return false;
		}
		if (!AvroUtils.isEqual(this.retentionperiod, other.retentionperiod)) {
			return false;
		}
		if (!AvroUtils.isEqual(this.deletionpolicy, other.deletionpolicy)) {
			return false;
		}
		return true;
	}

}
