package io.rtdi.bigdata.kafka.avro.recordbuilders;

import java.util.ArrayList;
import java.util.List;

public class FKCondition {

	private String name;
	private String schema_fqn;
	private List<JoinCondition> conditions;

	public FKCondition() {
	}

	/**
	 * A FK relationship has a name and the schema it points to.
	 *
	 * @param name arbitrary name of the relationship
	 * @param schema_fqn fully qualified name of the schema this FK points to
	 */
	public FKCondition(String name, String schema_fqn) {
		this();
		this.name = name;
		this.schema_fqn = schema_fqn;
		this.conditions = null;
	}

	public FKCondition(String name, String schema_fqn, String left, String right, String condition) {
		this(name, schema_fqn);
		addCondition(left, right, condition);
	}

	/**
	 * Add a created condition to the list of conditions.
	 *
	 * @param condition
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
	 * @param condition
	 * @return this
	 */
	public FKCondition withCondition(JoinCondition condition) {
		addCondition(condition);
		return this;
	}

	/**
	 * Shortcut for creating a join condition and adding it to the list of conditions.
	 *
	 * @param left
	 * @param right
	 * @param condition
	 */
	public void addCondition(String left, String right, String condition) {
		addCondition(new JoinCondition(left, right, condition));
	}

	/**
	 * Shortcut for creating a join condition and adding it to the list of conditions.
	 *
	 * @param left
	 * @param right
	 * @param condition
	 * @return this
	 */
	public FKCondition withCondition(String left, String right, String condition) {
		addCondition(left, right, condition);
		return this;
	}

	/**
	 * Set the list of conditions, overwriting any existing ones.
	 *
	 * @param conditions
	 */
	public void setConditions(List<JoinCondition> conditions) {
		this.conditions = conditions;
	}

	/**
	 * @return the name of the fk condition
	 */
	public String getName() {
		return name;
	}

	/**
	 * @return the fully qualified name of the schema this FK points to
	 */
	public String getSchemaFQN() {
		return schema_fqn;
	}

	/**
	 * Set the fully qualified name of the schema this FK points to
	 *
	 * @param schema_fqn
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
	 * @param name
	 */
	public void setName(String name) {
		this.name = name;
	}

	@Override
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

}
