package io.rtdi.bigdata.kafka.avro.recordbuilders;

/**
 * Examples of content sensitivity levels.
 */
public enum ContentSensitivity {
	/**
	 * This data is only for internal use within the organization
	 */
	INTERNAL,
	/**
	 * This data is only for use within a GBU
	 */
	GBU_ONLY,
	/**
	 * The data falls under ITAR regulations
	 */
	ITAR,
	/**
	 * The data falls under Dual Use regulations
	 */
	DUAL_USE,
	/**
	 * This data can be shown to everybody logged in
	 */
	PUBLIC,
	/**
	 * This data should not be shown to everybody logged in. The data itself is not the problem
	 * but in combination with other data personal information might be derived.
	 */
	SENSITIVE,
	/**
	 * This data by itself is highly sensitive, it is personal information.
	 * Examples like Social Security Number, Credit Card information, address details.
	 */
	PRIVATE,
	/**
	 * Data falling under certain regulations like EAR, Dual Use, ITAR and others
	 */
	REGULATED,
	/**
	 * Personally Identifiable Information
	 */
	PII,
	/**
	 * Protected Health Information
	 */
	PHI

}
