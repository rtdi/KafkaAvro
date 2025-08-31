package io.rtdi.bigdata.kafka.avro.recordbuilders;

import java.util.ArrayList;
import java.util.List;

public class FKCondition {

	private String name;
	private String schema_fqn;
	private List<JoinCondition> conditions;

	/**
	 * A FK relationship has a name and the schema it points to.
	 *
	 * @param name arbitrary name of the relationship
	 * @param schema_fqn fully qualified name of the schema this FK points to
	 */
	public FKCondition(String name, String schema_fqn) {
		super();
		this.name = name;
		this.schema_fqn = schema_fqn;
		this.conditions = null;
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
	 * @return all conditions that make up this FK relationship
	 */
	public List<JoinCondition> getConditions() {
		return conditions;
	}
}
