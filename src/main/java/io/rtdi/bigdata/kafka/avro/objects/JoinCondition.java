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
	 * Create an empty join condition
	 */
	/**
	 * Creates a new instance of this class.
	 */
	public JoinCondition() {
		super();
	}

	/**
	 * A join condition consists of a left and right side and a condition operator.
	 * Note that all strings are taken literally, so they can be field names, expressions or constants.
	 *
	 * @param left the expression related to the source schema
	 * @param right the expression related to the target schema
	 * @param condition is a SQL operator like =, &lt;, &gt;, &lt;=, &gt;=, &lt;&gt;
	 */
	/**
	 * Creates a new instance of this class.
	 * @param left the parameter value
	 * @param right the parameter value
	 * @param condition the parameter value
	 */
	public JoinCondition(String left, String right, String condition) {
		this();
		this.left = left;
		this.right = right;
		this.condition = condition;
	}

	/**
	 * @return left side expression
	 */
	/**
	 * Executes the String getLeft operation.
	 */
	public String getLeft() {
		return left;
	}

	/**
	 * @return the expression related to the target schema
	 */
	/**
	 * Executes the String getRight operation.
	 */
	public String getRight() {
		return right;
	}

	/**
	 * @return the condition string
	 */
	/**
	 * Executes the String getCondition operation.
	 */
	public String getCondition() {
		return condition;
	}

	@Override
	/**
	 * Executes the String toString operation.
	 */
	public String toString() {
		return left + " " + condition + " " + right;
	}

	/**
	 * Set the left side expression
	 *
	 * @param left the left side expression
	 */
	/**
	 * Executes the void setLeft operation.
	 * @param left the parameter value
	 */
	public void setLeft(String left) {
		this.left = left;
	}

	/**
	 * Set the right side expression
	 *
	 * @param right the right side expression
	 */
	/**
	 * Executes the void setRight operation.
	 * @param right the parameter value
	 */
	public void setRight(String right) {
		this.right = right;
	}

	/**
	 * Set the condition operator
	 *
	 * @param condition the condition operator
	 */
	/**
	 * Executes the void setCondition operation.
	 * @param condition the parameter value
	 */
	public void setCondition(String condition) {
		this.condition = condition;
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

	@Override
	/**
	 * Executes the int hashCode operation.
	 */
	public int hashCode() {
		return 1;
	}

}
