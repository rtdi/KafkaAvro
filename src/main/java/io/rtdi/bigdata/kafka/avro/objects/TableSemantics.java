package io.rtdi.bigdata.kafka.avro.objects;

import io.rtdi.bigdata.kafka.avro.AvroUtils;

public class TableSemantics {
	/**
	 * What is the main purpose of this table? Fact, dimension,... 
	 */
	public TableType type;

	/**
	 * Creates an empty table semantics instance.
	 */
	public TableSemantics() {
		super();
	}

	/**
	 * Creates table semantics with the given table type.
	 *
	 * @param type the table classification
	 */
	public TableSemantics(TableType type) {
		super();
		this.type = type;
	}

	/**
	 * Compares this table semantics instance to another object for equality.
	 *
	 * @param o the object to compare against
	 * @return {@code true} when the two instances are equivalent
	 */
	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || !(o instanceof TableSemantics)) {
			return false;
		}
		TableSemantics other = (TableSemantics) o;
		if (!AvroUtils.isEqual(this.type, other.type)) {
			return false;
		}
		return true;
	}

	/**
	 * Returns a stable hash code for the table semantics instance.
	 *
	 * @return the hash code for this instance
	 */
	@Override
	public int hashCode() {
		return 1;
	}

	/**
	 * Gets the table type classification.
	 *
	 * @return the table type
	 */
	public TableType getType() {
		return type;
	}

	/**
	 * Sets the table type classification.
	 *
	 * @param type the table type
	 */
	public void setType(TableType type) {
		this.type = type;
	}

}