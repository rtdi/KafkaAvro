package io.rtdi.bigdata.kafka.avro.objects;

import java.util.ArrayList;
import java.util.List;

import io.rtdi.bigdata.kafka.avro.AvroUtils;

/**
 * Foreign Key condition that points to another schema.
 */
public class FKCondition {

	private String name;
	private String schema_fqn;
	private List<JoinCondition> conditions;

	/**
	 * Default constructor
	 */
	/**
	 * Creates a new instance of this class.
	 */
	public FKCondition() {
	}

	/**
	 * A FK relationship has a name and the schema it points to.
	 *
	 * @param name arbitrary name of the relationship
	 * @param schema_fqn fully qualified name of the schema this FK points to
	 */
	/**
	 * Creates a new instance of this class.
	 * @param name the parameter value
	 * @param schema_fqn the parameter value
	 */
	public FKCondition(String name, String schema_fqn) {
		this();
		this.name = name;
		this.schema_fqn = schema_fqn;
		this.conditions = null;
	}

	/**
	 * Short hand for creating a FK relationship with one condition.
	 *
	 * @param name arbitrary name of the relationship
	 * @param schema_fqn fully qualified name of the schema this FK points to
	 * @param left the left side of the condition
	 * @param right the right side of the condition
	 * @param condition the condition operator, e.g. "="
	 */
	/**
	 * Creates a new instance of this class.
	 * @param name the parameter value
	 * @param schema_fqn the parameter value
	 * @param left the parameter value
	 * @param right the parameter value
	 * @param condition the parameter value
	 */
	public FKCondition(String name, String schema_fqn, String left, String right, String condition) {
		this(name, schema_fqn);
		addCondition(left, right, condition);
	}

	/**
	 * Add a created condition to the list of conditions.
	 *
	 * @param condition the condition to add
	 */
	/**
	 * Executes the void addCondition operation.
	 * @param condition the parameter value
	 */
	public void addCondition(JoinCondition condition) {
		if (conditions == null) {
			conditions = new ArrayList<>();
		}
		conditions.add(condition);
	}

	/**
	 * Add a created condition to the list of conditions.
	 *
	 * @param condition the condition to add
	 * @return this for chaining
	 */
	/**
	 * Executes the FKCondition withCondition operation.
	 * @param condition the parameter value
	 */
	public FKCondition withCondition(JoinCondition condition) {
		addCondition(condition);
		return this;
	}

	/**
	 * Shortcut for creating a join condition and adding it to the list of conditions.
	 *
	 * @param left the left side of the condition
	 * @param right the right side of the condition
	 * @param condition the condition operator, e.g. "="
	 */
	/**
	 * Executes the void addCondition operation.
	 * @param left the parameter value
	 * @param right the parameter value
	 * @param condition the parameter value
	 */
	public void addCondition(String left, String right, String condition) {
		addCondition(new JoinCondition(left, right, condition));
	}

	/**
	 * Shortcut for creating a join condition and adding it to the list of conditions.
	 *
	 * @param left left side of the condition
	 * @param right right side of the condition
	 * @param condition the condition operator, e.g. "="
	 * @return this for chaining
	 */
	/**
	 * Executes the FKCondition withCondition operation.
	 * @param left the parameter value
	 * @param right the parameter value
	 * @param condition the parameter value
	 */
	public FKCondition withCondition(String left, String right, String condition) {
		addCondition(left, right, condition);
		return this;
	}

	/**
	 * Set the list of conditions, overwriting any existing ones.
	 *
	 * @param conditions the list of conditions to set
	 */
	/**
	 * Executes the void setConditions operation.
	 * @param conditions the parameter value
	 */
	public void setConditions(List<JoinCondition> conditions) {
		this.conditions = conditions;
	}

	/**
	 * @return the name of the fk condition
	 */
	/**
	 * Executes the String getName operation.
	 */
	public String getName() {
		return name;
	}

	/**
	 * @return the fully qualified name of the schema this FK points to
	 */
	/**
	 * Executes the String getSchemaFQN operation.
	 */
	public String getSchemaFQN() {
		return schema_fqn;
	}

	/**
	 * Set the fully qualified name of the schema this FK points to
	 *
	 * @param schema_fqn the fully qualified name of the schema this FK points to
	 */
	/**
	 * Executes the void setSchemaFQN operation.
	 * @param schema_fqn the parameter value
	 */
	public void setSchemaFQN(String schema_fqn) {
		this.schema_fqn = schema_fqn;
	}

	/**
	 * @return all conditions that make up this FK relationship
	 */
	public List<JoinCondition> getConditions() {
		return conditions;
	}


	/**
	 * Set the name of this fk condition
	 *
	 * @param name the name of this fk condition
	 */
	/**
	 * Executes the void setName operation.
	 * @param name the parameter value
	 */
	public void setName(String name) {
		this.name = name;
	}

	@Override
	/**
	 * Executes the String toString operation.
	 */
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("FKCondition: ").append(name).append(" -> ").append(schema_fqn).append(" {");
		if (conditions != null) {
			boolean first = true;
			for (JoinCondition c : conditions) {
				if (first) {
					first = false;
				} else {
					sb.append(" and ");
				}
				sb.append(c.toString());
			}
		}
		sb.append("}");
		return sb.toString();
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
		if (o == null || !(o instanceof FKCondition)) {
			return false;
		}
		FKCondition other = (FKCondition) o;
		if (!AvroUtils.isEqual(this.name, other.name)) {
			return false;
		}
		if (!AvroUtils.isEqual(this.schema_fqn, other.schema_fqn)) {
			return false;
		}
		if (!AvroUtils.isEqual(this.conditions, other.conditions)) {
			return false;
		}
		return true;
	}

	@Override
	/**
	 * Executes the int hashCode operation.
	 */
	public int hashCode() {
		if (this.schema_fqn != null) {
			return this.schema_fqn.hashCode();
		} else {
			return 1;
		}
	}

}
