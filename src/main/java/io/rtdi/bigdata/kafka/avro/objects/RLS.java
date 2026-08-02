package io.rtdi.bigdata.kafka.avro.objects;

import io.rtdi.bigdata.kafka.avro.AvroUtils;

/**
 * 
 * RLS
 * 
 * Rowlevel security is implemented using a permission table. That table has a structure like
 * 
 * | dimension | value | username  |
 * +-----------+-------+-----------+
 * | region    | US    | user1     |
 * | region    | EMEA  | user2     |
 *
 * The data table will be joined with the permission table like
 * select * from SALES
 * where SALES_REGION in (select value from permission_table where username=user() and dimension = &lt;dimension&gt;)
 *
 */
public class RLS {
	/**
	 * The dimension name to be used in the permission table
	 */
	public String dimension;
	/**
	 * The field name in this schema that holds the dimension's value, e.g. data_table's SALES_REGION column
	 */
	public String field;

	/**
	 * Creates an empty row-level security definition.
	 */
	public RLS() {
		super();
	}

	/**
	 * Creates a row-level security definition for a specific dimension and field.
	 *
	 * @param dimension the permission-table dimension name
	 * @param field the schema field that carries the dimension value
	 */
	public RLS(String dimension, String field) {
		super();
		this.dimension = dimension;
		this.field = field;
	}

	/**
	 * Gets the permission-table dimension name.
	 *
	 * @return the dimension name
	 */
	public String getDimension() {
		return dimension;
	}

	/**
	 * Sets the permission-table dimension name.
	 *
	 * @param dimension the dimension name
	 */
	public void setDimension(String dimension) {
		this.dimension = dimension;
	}

	/**
	 * Gets the schema field that carries the dimension value.
	 *
	 * @return the field name
	 */
	public String getField() {
		return field;
	}

	/**
	 * Sets the schema field that carries the dimension value.
	 *
	 * @param field the field name
	 */
	public void setField(String field) {
		this.field = field;
	}

	/**
	 * Compares this row-level security entry to another object for equality.
	 *
	 * @param o the object to compare against
	 * @return {@code true} when the two entries are equivalent
	 */
	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || !(o instanceof RLS)) {
			return false;
		}
		RLS other = (RLS) o;
		if (!AvroUtils.isEqual(this.dimension, other.dimension)) {
			return false;
		}
		if (!AvroUtils.isEqual(this.field, other.field)) {
			return false;
		}
		return true;
	}

	/**
	 * Returns a stable hash code for the row-level security entry.
	 *
	 * @return the hash code for this instance
	 */
	@Override
	public int hashCode() {
		return 1;
	}

}