package io.rtdi.bigdata.kafka.avro.recordbuilders;

public enum ContentSensitivity {
	INTERNAL,
	GBU_ONLY,
	ITAR,
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
