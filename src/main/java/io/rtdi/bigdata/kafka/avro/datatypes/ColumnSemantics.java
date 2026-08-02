package io.rtdi.bigdata.kafka.avro.datatypes;

import io.rtdi.bigdata.kafka.avro.AvroUtils;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroField.ColumnType;

/**
 * ColumnSemantics provide input to what the column is used for, e.g. a measure, a hierarchy, etc.
 */
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
	 * get the type of this column
	 * @return the type
	 */
	public ColumnType getType() {
		return type;
	}

    /**
     * set the type of this column
     * @param type the parameter value
     */
    public void setType(ColumnType type) {
		this.type = type;
	}

	/**
	 * get the aggregation formula for this column
	 * @return the aggregation formula
	 */
	public String getAggregation_formula() {
		return aggregation_formula;
	}

	/**
	 * set the aggregation formula for this column
	 * @param aggregation_formula the parameter value
	 */
	public void setAggregation_formula(String aggregation_formula) {
		this.aggregation_formula = aggregation_formula;
	}

	/**
	 * get the currency field name for this column
	 * @return the currency field name
	 */
	public String getCurrency_field_name() {
		return currency_field_name;
	}

	/**
	 * set the currency field name for this column
	 * @param currency_field_name the parameter value
	 */
	public void setCurrency_field_name(String currency_field_name) {
		this.currency_field_name = currency_field_name;
	}

	/**
	 * get the currency conversion date for this column
	 * @return the currency conversion date
	 */
	public String getCurrency_conversion_date() {
		return currency_conversion_date;
	}

	/**
	 * set the currency conversion date for this column
	 * @param currency_conversion_date the parameter value
	 */
	public void setCurrency_conversion_date(String currency_conversion_date) {
		this.currency_conversion_date = currency_conversion_date;
	}

	/**
	 * get the unit of measure field name for this column
	 * @return the unit of measure field name
	 */
	public String getUom_field_name() {
		return uom_field_name;
	}

	/**
	 * set the unit of measure field name for this column
	 * @param uom_field_name the parameter value
	 */
	public void setUom_field_name(String uom_field_name) {
		this.uom_field_name = uom_field_name;
	}

	/**
	 * get the hierarchy name for this column
	 * @return the hierarchy name
	 */
	public String getHierarchy_name() {
		return hierarchy_name;
	}

	/**
	 * set the hierarchy name for this column
	 * @param hierarchy_name the parameter value
	 */
	public void setHierarchy_name(String hierarchy_name) {
		this.hierarchy_name = hierarchy_name;
	}

    /**
     * get the hierarchy level for this column
     * @return the hierarchy level
     */
    public Integer getHierarchy_level() {
		return hierarchy_level;
	}

	/**
	 * set the hierarchy level for this column
	 * @param hierarchy_level the parameter value
	 */
	public void setHierarchy_level(Integer hierarchy_level) {
		this.hierarchy_level = hierarchy_level;
	}



	@Override
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
	public int hashCode() {
		return 1;
	}

}