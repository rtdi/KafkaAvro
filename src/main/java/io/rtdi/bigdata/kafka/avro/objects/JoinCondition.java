package io.rtdi.bigdata.kafka.avro.objects;

import io.rtdi.bigdata.kafka.avro.AvroUtils;

/**
 * Defines a join condition between two schemas.
 */
public class JoinCondition {
	private String left;
	private String right;
	private String condition;

	/**
	 * Creates an empty join condition.
	 */
	public JoinCondition() {
		super();
	}

	/**
	 * Creates a join condition using a left expression, a right expression, and a comparison operator.
	 *
	 * @param left the expression related to the source schema
	 * @param right the expression related to the target schema
	 * @param condition the SQL operator such as {@code =}, {@code <}, {@code >}, {@code <=}, {@code >=}, or {@code <>}
	 */
	public JoinCondition(String left, String right, String condition) {
		this();
		this.left = left;
		this.right = right;
		this.condition = condition;
	}

	/**
	 * Gets the left-side expression of the join condition.
	 *
	 * @return the left expression
	 */
	public String getLeft() {
		return left;
	}

	/**
	 * Gets the right-side expression of the join condition.
	 *
	 * @return the right expression
	 */
	public String getRight() {
		return right;
	}

	/**
	 * Gets the comparison operator used by the join condition.
	 *
	 * @return the condition operator
	 */
	public String getCondition() {
		return condition;
	}

	/**
	 * Returns a readable string representation of the join condition.
	 *
	 * @return the join condition text
	 */
	@Override
	public String toString() {
		return left + " " + condition + " " + right;
	}

	/**
	 * Sets the left-side expression of the join condition.
	 *
	 * @param left the left expression
	 */
	public void setLeft(String left) {
		this.left = left;
	}

	/**
	 * Sets the right-side expression of the join condition.
	 *
	 * @param right the right expression
	 */
	public void setRight(String right) {
		this.right = right;
	}

	/**
	 * Sets the comparison operator used by the join condition.
	 *
	 * @param condition the condition operator
	 */
	public void setCondition(String condition) {
		this.condition = condition;
	}

	/**
	 * Compares this join condition to another object for equality.
	 *
	 * @param o the object to compare against
	 * @return {@code true} when the two join conditions are equivalent
	 */
	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || !(o instanceof JoinCondition)) {
			return false;
		}
		JoinCondition other = (JoinCondition) o;
		if (!AvroUtils.isEqual(this.left, other.left)) {
			return false;
		}
		if (!AvroUtils.isEqual(this.right, other.right)) {
			return false;
		}
		if (!AvroUtils.isEqual(this.condition, other.condition)) {
			return false;
		}
		return true;
	}

	/**
	 * Returns a stable hash code for the join condition.
	 *
	 * @return the hash code for this instance
	 */
	@Override
	public int hashCode() {
		return 1;
	}

}
