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

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
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
	public static final String NAME = "VALUESCHEMA";

	/**
	 * Transform result quality column name
	 */
	public static final String AUDIT_TRANSFORMRESULT_QUALITY = "__transformresult_quality";
	/**
	 * Transform result text column name
	 */
	public static final String AUDITTRANSFORMRESULTTEXT = "__transformresult_text";
	/**
	 * Transformation name column name
	 */
	public static final String AUDITTRANSFORMATIONNAME = "__transformationname";
	/**
	 * Audit details column name
	 */
	public static final String AUDITDETAILS = "__details";
	/**
	 * Transform result column name
	 */
	public static final String TRANSFORMRESULT = "__transformresult";
	/**
	 * Audit column name
	 */
	public static final String AUDIT = "__audit";
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

	public static final String SCHEMA_INFO_OBJECT_LEVEL_SECURITY = "object_level_security";
	public static final String SCHEMA_INFO_ROW_LEVEL_SECURITY = "row_level_security";
	public static final String SCHEMA_INFO_PARTITION_BY = "partition_by";
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
	 * Creates a new instance of this class.
	 * @param name the parameter value
	 * @param namespace the parameter value
	 * @param description the parameter value
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
		RecordSchema extension = new RecordSchema("__extension", "Extension point to add custom values to each record");
		extension.add("__path", AvroString.create(), "An unique identifier, e.g. \"street\".\"house number component\"", false);
		extension.add("__value", AvroString.create(), "The value of any primitive datatype of Avro", false);
		add(SchemaConstants.SCHEMA_COLUMN_EXTENSION, new AvroArray(extension), "Add more columns beyond the official logical data model", true).setInternal(true);
        add(SchemaConstants.SCHEMA_COLUMN_EXTENSION_MAP, new AvroMap(AvroString.create()), "Add more values as a map beyond the official logical data model", true).setInternal(true);

		RecordSchema audit = new RecordSchema(AUDIT, "If data is transformed this information is recorded here");
		audit.add(TRANSFORMRESULT, AvroVarchar.create(4), "Is the record PASS, FAILED or WARN?", false);
		RecordSchema audit_details = new RecordSchema("__audit_details", "Details of all transformations");
		audit_details.add(AUDITTRANSFORMATIONNAME, AvroNVarchar.create(1024), "A name identifying the applied transformation", false);
		audit_details.add(TRANSFORMRESULT, AvroVarchar.create(4), "Is the record PASS, FAIL or WARN?", false);
		audit_details.add(AUDITTRANSFORMRESULTTEXT, AvroNVarchar.create(1024), "Transforms can optionally describe what they did", true);
		audit_details.add(AUDIT_TRANSFORMRESULT_QUALITY, AvroByte.create(), "Transforms can optionally return a percent value from 0 (FAIL) to 100 (PASS)", true);
		audit.add(AUDITDETAILS, new AvroArray(audit_details), "Details of all transformations", true);

		add(AUDIT, audit, "If data is transformed this information is recorded here", true).setInternal(true);
	}

		/**
	 * @param name of the value schema
	 * @param description free for text
	 * @throws SchemaBuilderException if the schema is invalid
	 * @see #ValueSchema(String, String, String)
	 */
	/**
	 * Creates a new instance of this class.
	 * @param name the parameter value
	 * @param description the parameter value
	 */
	public ValueSchema(String name, String description) throws SchemaBuilderException {
		this(name, null, description);
	}

	/**
	 * Creates a new instance of this class.
	 */
	public ValueSchema() {
		super();
	}

	/**
	 * Creates a new instance of this class.
	 * @param schema the parameter value
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
	     * Executes the String getType operation.
	     */
	    public String getType() {
        return NAME;
    }

	/**
	 * Add regulations that apply to this schema, e.g. GDPR, HIPAA, CCPA, ...
	 *
	 * @param regulations list of regulations
	 */
	/**
	 * Executes the void setRegulations operation.
	 * @param regulations the parameter value
	 */
	public void setRegulations(Collection<String> regulations) {
		this.regulations = regulations;
	}

	/**
	 * Add regulations that apply to this schema, e.g. GDPR, HIPAA, CCPA, ...
	 *
	 * @param regulations list of regulations
	 */
	@JsonIgnore
	/**
	 * Executes the void setRegulations operation.
	 * @param regulations the parameter value
	 */
	public void setRegulations(String... regulations) {
		this.regulations = Arrays.asList(regulations);
	}

	/**
	 * @return the regulations that apply to this schema, e.g. GDPR, HIPAA, CCPA, ...
	 */
	@JsonGetter(SCHEMA_INFO_REGULATIONS)
	public Collection<String> getRegulations() {
		return regulations;
	}

	/**
	 * Set the url of the ticket system where issues can be reported
	 *
	 * @param url as string - not validated
	 */
	/**
	 * Executes the void setTicketUrl operation.
	 * @param url the parameter value
	 */
	public void setTicketUrl(String url) {
		this.ticketurl = url;
	}

	/**
	 * Get the url of the ticket system where issues can be reported
	 *
	 * @return url as string - not validated
	 */
	@JsonGetter(SCHEMA_INFO_TICKETS_URL)
	/**
	 * Executes the String getTicketUrl operation.
	 */
	public String getTicketUrl() {
		return ticketurl;
	}

	/**
	 * Set the repository url where the code for this data product is located
	 *
	 * @param url as string - not validated
	 */
	/**
	 * Executes the void setRepoUrl operation.
	 * @param url the parameter value
	 */
	public void setRepoUrl(String url) {
		this.repourl = url;
	}

	/**
	 * Get the repository url where the code for this data product is located
	 *
	 * @return url as string - not validated
	 */
	@JsonGetter(SCHEMA_INFO_REPO_URL)
	/**
	 * Executes the String getRepoUrl operation.
	 */
	public String getRepoUrl() {
		return repourl;
	}


	/**
	 * Executes the void setObjectLevelSecurity operation.
	 * @param object_level_security the parameter value
	 */
	public void setObjectLevelSecurity(List<String> object_level_security) {
		this.objectlevelsecurity = object_level_security;
	}

	@JsonIgnore
	/**
	 * Executes the void setObjectLevelSecurity operation.
	 * @param object_level_security the parameter value
	 */
	public void setObjectLevelSecurity(String... object_level_security) {
		this.objectlevelsecurity = Arrays.asList(object_level_security);
	}

	@JsonGetter(SCHEMA_INFO_OBJECT_LEVEL_SECURITY)
	public List<String> getObjectLevelSecurity() {
		return this.objectlevelsecurity;
	}

	/**
	 * Add one RowLevelSecurity entry to the list of existing entries.
	 * @param dimension the dimension name to be used in the permission table
	 * @param field the field name in this schema that holds the dimension's value, e.g. data_table's SALES_REGION column
	 */
	/**
	 * Executes the void addRowLevelSecurity operation.
	 * @param dimension the parameter value
	 * @param field the parameter value
	 */
	public void addRowLevelSecurity(String dimension, String field) {
		if (row_level_security == null) {
			row_level_security = new ArrayList<>();
		}
		row_level_security.add(new RLS(dimension, field));
	}

	/**
	 * Executes the void setRowLevelSecurity operation.
	 * @param row_level_security the parameter value
	 */
	public void setRowLevelSecurity(List<RLS> row_level_security) {
		this.row_level_security = row_level_security;
	}

	@JsonGetter(SCHEMA_INFO_ROW_LEVEL_SECURITY)
	public List<RLS> getRowLevelSecurity() {
		return this.row_level_security;
	}

	/**
	 * Executes the void setPartitionBy operation.
	 * @param partition_by the parameter value
	 */
	public void setPartitionBy(List<String> partition_by) {
		this.partition_by = partition_by;
	}

	@JsonGetter(SCHEMA_INFO_PARTITION_BY)
	public List<String> getPartitionBy() {
		return partition_by;
	}

	/**
	 * Executes the void setSemantics operation.
	 * @param semantics the parameter value
	 */
	public void setSemantics(TableSemantics semantics) {
		this.semantics = semantics;
	}
	
	@JsonGetter(SCHEMA_INFO_SEMANTICS)
	/**
	 * Executes the TableSemantics getSemantics operation.
	 */
	public TableSemantics getSemantics() {
		return this.semantics;
	}

	/**
	 * Set the email address of the data product owner
	 *
	 * @param email as string - not validated
	 */
	/**
	 * Executes the void setDataProductOwner operation.
	 * @param email the parameter value
	 */
	public void setDataProductOwner(String email) {
		this.dataproductowner = email;
	}

	/**
	 * Get the email address of the data product owner
	 *
	 * @return email as string - not validated
	 */
	@JsonGetter(SCHEMA_INFO_DATAPRODUCT_OWNER)
	/**
	 * Executes the String getDataProductOwner operation.
	 */
	public String getDataProductOwner() {
		return this.dataproductowner;
	}


	/**
	 * Set the primary key columns of this schema.
	 *
	 * @param columnnames to be used in the root schema as primary key
	 */
	@JsonIgnore
	/**
	 * Executes the void setPrimaryKeys operation.
	 * @param columnnames the parameter value
	 */
	public void setPrimaryKeys(String... columnnames) {
		this.pks = Arrays.asList(columnnames);
	}

	/**
	 * Executes the void setPrimaryKeys operation.
	 * @param columnnames the parameter value
	 */
	public void setPrimaryKeys(List<String> columnnames) {
		this.pks = columnnames;
	}

	@JsonGetter(PRIMARY_KEYS)
	public List<String> getPrimaryKeys() {
		return this.pks;
	}

	/**
	 * Add a foreign key relationship to another schema.
	 *
	 * @param condition the FKCondition to add
	 */
	/**
	 * Executes the void addForeignKey operation.
	 * @param condition the parameter value
	 */
	public void addForeignKey(FKCondition condition) {
		if (fks == null) {
			fks = new ArrayList<>();
		}
		fks.add(condition);
	}

	/**
	 * Executes the void setForeignKeys operation.
	 * @param fks the parameter value
	 */
	public void setForeignKeys(List<FKCondition> fks) {
		this.fks = fks;
	}

	@JsonGetter(FOREIGN_KEYS)
	public List<FKCondition> getForeignKeys() {
		return this.fks;
	}

	/**
	 * Add a simple foreign key relationship to another schema.
	 *
	 * @param name name of the FK
	 * @param schema_fqn target schema fully qualified name
	 * @param left left side column name
	 * @param right right side column name
	 * @param condition optional condition, e.g. "AND enddate IS NULL"
	 */
	/**
	 * Executes the void addForeignKey operation.
	 * @param name the parameter value
	 * @param schema_fqn the parameter value
	 * @param left the parameter value
	 * @param right the parameter value
	 * @param condition the parameter value
	 */
	public void addForeignKey(String name, String schema_fqn, String left, String right, String condition) {
		FKCondition fk = new FKCondition(name, schema_fqn, left, right, condition);
		addForeignKey(fk);
	}

	/**
	 * Hint the retention period for this data
	 *
	 * @param period as Duration
	 */
	/**
	 * Executes the void setRetentionPeriod operation.
	 * @param period the parameter value
	 */
	public void setRetentionPeriod(Duration period) {
		this.retentionperiod = period;
	}

	/**
	 * @return the retention period for this data or null if not set
	 */
	@JsonGetter(SCHEMA_INFO_RETENTION_PERIOD)
	/**
	 * Executes the Duration getRetentionPeriod operation.
	 */
	public Duration getRetentionPeriod() {
		return this.retentionperiod;
	}

	/**
	 * Hint the deletion policy for this data
	 *
	 * @param policy the deletion policy
	 */
	/**
	 * Executes the void setDeletionPolicy operation.
	 * @param policy the parameter value
	 */
	public void setDeletionPolicy(DeletionPolicy policy) {
		this.deletionpolicy = policy;
	}

	/**
	 * @return the deletion policy for this data or null if not set
	 */
	@JsonGetter(SCHEMA_INFO_DELETION_POLICY)
	/**
	 * Executes the DeletionPolicy getDeletionPolicy operation.
	 */
	public DeletionPolicy getDeletionPolicy() {
		return this.deletionpolicy;
	}

	@Override
	/**
	 * Executes the Schema createSchema operation.
	 */
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
	 * Executes the String toAvroJson operation.
	 */
	public String toAvroJson() {
		return SchemaFormatter.format("json/pretty", this.createSchema());
	}

	/**
	 * Executes the String toObjectJson operation.
	 */
	public String toObjectJson() throws JsonProcessingException {
		return om.writeValueAsString(this);
	}

	/**
	 * Executes the ValueSchema fromObjectJson operation and returns the resulting value.
	 * @param json the parameter value
	 * @return the resulting value
	 */
	public static ValueSchema fromObjectJson(String json) throws JsonMappingException, JsonProcessingException {
		ObjectMapper om = AvroUtils.createJacksonOM();
		return om.readValue(json, ValueSchema.class);
	}

	@Override
	/**
	 * Executes the boolean equals operation.
	 * @param o the parameter value
	 */
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
