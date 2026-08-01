package io.rtdi.bigdata.kafka.avro.objects;

import io.rtdi.bigdata.kafka.avro.AvroUtils;

public class TableSemantics {
	/**
	 * What is the main purpose of this table? Fact, dimension,... 
	 */
	public TableType type;

	/**
	 * Creates a new instance of this class.
	 */
	public TableSemantics() {
		super();
	}

	/**
	 * Creates a new instance of this class.
	 * @param type the parameter value
	 */
	public TableSemantics(TableType type) {
		super();
		this.type = type;
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
		if (o == null || !(o instanceof TableSemantics)) {
			return false;
		}
		TableSemantics other = (TableSemantics) o;
		if (!AvroUtils.isEqual(this.type, other.type)) {
			return false;
		}
		return true;
	}

	@Override
	/**
	 * Executes the int hashCode operation.
	 */
	public int hashCode() {
		return 1;
	}

	/**
	 * Executes the TableType getType operation.
	 */
	public TableType getType() {
		return type;
	}

	/**
	 * Executes the void setType operation.
	 * @param type the parameter value
	 */
	public void setType(TableType type) {
		this.type = type;
	}

}