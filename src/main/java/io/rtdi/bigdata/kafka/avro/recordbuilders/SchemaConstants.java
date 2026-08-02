package io.rtdi.bigdata.kafka.avro.recordbuilders;

/**
 * Some constants for schema field names
 */
public class SchemaConstants {

	/**
	 * Used to identify the change that caused this message in the source system, like a database global transaction id
	 */
	public static final String SCHEMA_COLUMN_SOURCE_TRANSACTION = "__source_transaction";
	/**
	 * The location in the source this change can be found in, e.g. in a file the line number
	 */
	public static final String SCHEMA_COLUMN_SOURCE_ROWID = "__source_rowid";
	/**
	 * The time in millis UTC this change was produced using the producer's clock. Usually System.getMillis().
	 */
	public static final String SCHEMA_COLUMN_CHANGE_TIME = "__change_time";
	/**
	 * An indicator of what kind of change this change message caused, an insert, update, delete,...
	 */
	public static final String SCHEMA_COLUMN_CHANGE_TYPE = "__change_type";
	/**
	 * The producer source system name or the producer name.
	 */
	public static final String SCHEMA_COLUMN_SOURCE_SYSTEM = "__source_system";
	/**
	 * Truncate information in case the change type is truncate.
	 */
	public static final String SCHEMA_COLUMN_TRUNCATE = "__truncate";
	/**
	 * A place for adding key/value pairs to a message.
	 */
	public static final String SCHEMA_COLUMN_EXTENSION_MAP = "__extension_map";
    /**
     * Audit column name
     */
    public static final String AUDIT = "__audit";
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
}
