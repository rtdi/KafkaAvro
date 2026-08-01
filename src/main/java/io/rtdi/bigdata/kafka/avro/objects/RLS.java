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
 * where SALES_REGION in (select value from permission_table where username=user() and dimension = <dimension>)
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
	 * Creates a new instance of this class.
	 */
	public RLS() {
		super();
	}

	/**
	 * Creates a new instance of this class.
	 * @param dimension the parameter value
	 * @param field the parameter value
	 */
	public RLS(String dimension, String field) {
		super();
		this.dimension = dimension;
		this.field = field;
	}

			/**
			 * Executes the String getDimension operation.
			 */
			public String getDimension() {
		return dimension;
	}

	/**
	 * Executes the void setDimension operation.
	 * @param dimension the parameter value
	 */
	public void setDimension(String dimension) {
		this.dimension = dimension;
	}

	/**
	 * Executes the String getField operation.
	 */
	public String getField() {
		return field;
	}

	/**
	 * Executes the void setField operation.
	 * @param field the parameter value
	 */
	public void setField(String field) {
		this.field = field;
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

	@Override
	/**
	 * Executes the int hashCode operation.
	 */
	public int hashCode() {
		return 1;
	}

}