package io.rtdi.bigdata.kafka.avro.datatypes;

import io.rtdi.bigdata.kafka.avro.AvroUtils;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroField.ColumnType;

public class ColumnSemantics {
	private ColumnType type;
	/**
	 * A SQL formula used to aggregate the values, e.g. sum(AMOUNT) or sum(BALANCE)/count(distinct BOOKING_DATE)
	 */
	private String aggregation_formula = null;
	/**
	 * With field of the table contains the currency information for this amount column
	 */
	private String currency_field_name = null;
	/**
	 * The field name used to convert the currency, e.g. VALUTA_DATE
	 */
	private String currency_conversion_date = null;
	/**
	 * With field of the table contains the unit of measure information for this amount column
	 */
	private String uom_field_name = null;
	/**
	 * The name of the hierarchy this field is part of
	 */
	private String hierarchy_name = null;
	/**
	 * The level of the hierarchy this field is part of, starting with 1
	 */
	private Integer hierarchy_level = null;

	/**
	 * Creates a new instance of this class.
	 */
	public ColumnSemantics() {
	}

	/**
	 * Creates a new instance of this class.
	 * @param type the parameter value
	 */
	public ColumnSemantics(ColumnType type) {
		this.type = type;
	}

	/**
	 * Creates a new instance of this class.
	 * @param aggregation_formula the parameter value
	 */
	public ColumnSemantics(String aggregation_formula) {
		this(ColumnType.MEASURE);
		this.aggregation_formula = aggregation_formula;
	}

	/**
	 * Creates a new instance of this class.
	 * @param aggregation_formula the parameter value
	 * @param currency_field_name the parameter value
	 * @param currency_conversion_date the parameter value
	 */
	public ColumnSemantics(String aggregation_formula, String currency_field_name, String currency_conversion_date) {
		this(aggregation_formula);
		this.aggregation_formula = aggregation_formula;
		this.currency_conversion_date = currency_conversion_date;
		this.currency_field_name = currency_field_name;
	}

	/**
	 * Creates a new instance of this class.
	 * @param aggregation_formula the parameter value
	 * @param uom_field_name the parameter value
	 */
	public ColumnSemantics(String aggregation_formula, String uom_field_name) {
		this(aggregation_formula);
		this.uom_field_name = uom_field_name;
	}

	/**
	 * Creates a new instance of this class.
	 * @param hierarchy_name the parameter value
	 * @param hierarchy_level the parameter value
	 */
	public ColumnSemantics(String hierarchy_name, Integer hierarchy_level) {
		this(ColumnType.HIERARCHY);
		this.hierarchy_name = hierarchy_name;
		this.hierarchy_level = hierarchy_level;
	}

	/**
	 * Executes the ColumnType getType operation.
	 */
	public ColumnType getType() {
		return type;
	}

    /**
     * Executes the void setType operation.
     * @param type the parameter value
     */
    public void setType(ColumnType type) {
		this.type = type;
	}

	/**
	 * Executes the String getAggregation_formula operation.
	 */
	public String getAggregation_formula() {
		return aggregation_formula;
	}

	/**
	 * Executes the void setAggregation_formula operation.
	 * @param aggregation_formula the parameter value
	 */
	public void setAggregation_formula(String aggregation_formula) {
		this.aggregation_formula = aggregation_formula;
	}

	/**
	 * Executes the String getCurrency_field_name operation.
	 */
	public String getCurrency_field_name() {
		return currency_field_name;
	}

	/**
	 * Executes the void setCurrency_field_name operation.
	 * @param currency_field_name the parameter value
	 */
	public void setCurrency_field_name(String currency_field_name) {
		this.currency_field_name = currency_field_name;
	}

	/**
	 * Executes the String getCurrency_conversion_date operation.
	 */
	public String getCurrency_conversion_date() {
		return currency_conversion_date;
	}

	/**
	 * Executes the void setCurrency_conversion_date operation.
	 * @param currency_conversion_date the parameter value
	 */
	public void setCurrency_conversion_date(String currency_conversion_date) {
		this.currency_conversion_date = currency_conversion_date;
	}

	/**
	 * Executes the String getUom_field_name operation.
	 */
	public String getUom_field_name() {
		return uom_field_name;
	}

	/**
	 * Executes the void setUom_field_name operation.
	 * @param uom_field_name the parameter value
	 */
	public void setUom_field_name(String uom_field_name) {
		this.uom_field_name = uom_field_name;
	}

	/**
	 * Executes the String getHierarchy_name operation.
	 */
	public String getHierarchy_name() {
		return hierarchy_name;
	}

	/**
	 * Executes the void setHierarchy_name operation.
	 * @param hierarchy_name the parameter value
	 */
	public void setHierarchy_name(String hierarchy_name) {
		this.hierarchy_name = hierarchy_name;
	}

    /**
     * Executes the Integer getHierarchy_level operation.
     */
    public Integer getHierarchy_level() {
		return hierarchy_level;
	}

	/**
	 * Executes the void setHierarchy_level operation.
	 * @param hierarchy_level the parameter value
	 */
	public void setHierarchy_level(Integer hierarchy_level) {
		this.hierarchy_level = hierarchy_level;
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
		if (o == null || !(o instanceof ColumnSemantics)) {
			return false;
		}
		ColumnSemantics other = (ColumnSemantics) o;
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

}