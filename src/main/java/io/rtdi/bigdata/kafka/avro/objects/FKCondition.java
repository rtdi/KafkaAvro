package io.rtdi.bigdata.kafka.avro.objects;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import io.rtdi.bigdata.kafka.avro.AvroUtils;

/**
 * Foreign Key condition that points to another schema.
 */
public class FKCondition {

	private String name;
	private String schema_fqn;
	private List<JoinCondition> conditions;

	/**
	 * Creates an empty foreign-key condition.
	 */
	public FKCondition() {
	}

	/**
	 * Creates a foreign-key condition with a relationship name and target schema.
	 *
	 * @param name the arbitrary name of the relationship
	 * @param schema_fqn the fully qualified name of the target schema
	 */
	public FKCondition(String name, String schema_fqn) {
		this();
		this.name = name;
		this.schema_fqn = schema_fqn;
		this.conditions = null;
	}

	/**
	 * Creates a foreign-key condition with a single join condition.
	 *
	 * @param name the arbitrary name of the relationship
	 * @param schema_fqn the fully qualified name of the target schema
	 * @param left the left side of the condition
	 * @param right the right side of the condition
	 * @param condition the condition operator, for example "="
	 */
	public FKCondition(String name, String schema_fqn, String left, String right, String condition) {
		this(name, schema_fqn);
		addCondition(left, right, condition);
	}

	/**
	 * Adds a join condition to the foreign-key relationship.
	 *
	 * @param condition the join condition to add
	 */
	public void addCondition(JoinCondition condition) {
		if (conditions == null) {
			conditions = new ArrayList<>();
		}
		conditions.add(condition);
	}

	/**
	 * Adds a join condition and returns this instance for chaining.
	 *
	 * @param condition the join condition to add
	 * @return this foreign-key condition instance
	 */
	public FKCondition withCondition(JoinCondition condition) {
		addCondition(condition);
		return this;
	}

	/**
	 * Creates a join condition from the provided expressions and adds it to the relationship.
	 *
	 * @param left the left side of the condition
	 * @param right the right side of the condition
	 * @param condition the condition operator, for example "="
	 */
	public void addCondition(String left, String right, String condition) {
		addCondition(new JoinCondition(left, right, condition));
	}

	/**
	 * Creates a join condition from the provided expressions, adds it to the relationship, and returns this instance.
	 *
	 * @param left the left side of the condition
	 * @param right the right side of the condition
	 * @param condition the condition operator, for example "="
	 * @return this foreign-key condition instance
	 */
	public FKCondition withCondition(String left, String right, String condition) {
		addCondition(left, right, condition);
		return this;
	}

	/**
	 * Replaces the current list of join conditions.
	 *
	 * @param conditions the new list of conditions
	 */
	public void setConditions(List<JoinCondition> conditions) {
		this.conditions = conditions;
	}

	/**
	 * Gets the name of the foreign-key condition.
	 *
	 * @return the foreign-key condition name
	 */
	public String getName() {
		return name;
	}

	/**
	 * Gets the fully qualified name of the target schema.
	 *
	 * @return the target schema name
	 */
	public String getSchemaFQN() {
		return schema_fqn;
	}

	/**
	 * Sets the fully qualified name of the target schema.
	 *
	 * @param schema_fqn the target schema name
	 */
	public void setSchemaFQN(String schema_fqn) {
		this.schema_fqn = schema_fqn;
	}

	/**
	 * Gets all join conditions that make up the foreign-key relationship.
	 *
	 * @return the join conditions
	 */
	public List<JoinCondition> getConditions() {
		return conditions;
	}


	/**
	 * Sets the name of this foreign-key condition.
	 *
	 * @param name the foreign-key condition name
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * Returns a readable description of the foreign-key condition.
	 *
	 * @return the foreign-key condition text
	 */
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

	/**
	 * Compares this foreign-key condition to another object for equality.
	 *
	 * @param o the object to compare against
	 * @return {@code true} when the two objects are equivalent
	 */
	@Override
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

	/**
	 * Returns a stable hash code for this foreign-key condition.
	 *
	 * @return the hash code for this instance
	 */
	@Override
	public int hashCode() {
		if (this.schema_fqn != null) {
			return Objects.hashCode(this.schema_fqn);
		} else {
			return 1;
		}
	}

}
