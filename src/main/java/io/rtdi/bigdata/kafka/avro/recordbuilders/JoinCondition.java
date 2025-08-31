package io.rtdi.bigdata.kafka.avro.recordbuilders;

public class JoinCondition {
	private String left;
	private String right;
	private String condition;

	/**
	 * A join condition consists of a left and right side and a condition operator.
	 * Note that all strings are taken literally, so they can be field names, expressions or constants.
	 *
	 * @param left
	 * @param right
	 * @param condition is a SQL operator like =, <, >, <=, >=, <>
	 */
	public JoinCondition(String left, String right, String condition) {
		super();
		this.left = left;
		this.right = right;
		this.condition = condition;
	}

	/**
	 * @return left side expression
	 */
	public String getLeft() {
		return left;
	}

	/**
	 * @return the expression related to the target schema
	 */
	public String getRight() {
		return right;
	}

	/**
	 * @return the condition string
	 */
	public String getCondition() {
		return condition;
	}

	@Override
	public String toString() {
		return left + " " + condition + " " + right;
	}
}
