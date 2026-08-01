package io.rtdi.bigdata.kafka.avro.objects;

public enum TableType {
	/**
	 * This is primarily a fact table
	 */
	FACT,
	/**
	 * This is primarily a dimension table
	 */
	DIMENSION,
	/**
	 * This table has the character of a dimension and a fact
	 */
	FACT_DIMENSION,
	/**
	 * A table with ID and LANGUAGE as logical primary key and a test field
	 */
	MULTILANGUAGE_TEXT,
	/**
	 * Contains a hierarchy in form of a parent child table
	 */
	PARENT_CHILD_HIERARCHY,
	/**
	 * A table used to convert one currency into another
	 */
	CURRENCY_CONVERSION,
	/**
	 * A table used to convert one unit of measure into another, e.g. kg into tons
	 */
	UOM_CONVERSION,
	/**
	 * A technical table used to resolve m:n relationships
	 */
	BRIDGE_TABLE,
	/**
	 * Maps a value of one system to another system, e.g. a SAP customer number to a Salesforce customer id
	 */
	VALUE_MAPPING,
	/**
	 * Mark this table as internal - not meant to be used by anybody but data engineers
	 */
	INTERNAL,
	/**
	 * This table contains permission related information, e.g. users, roles, role assignments, row level security filters
	 */
	SECURITY
}