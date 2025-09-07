package io.rtdi.bigdata.kafka.avro.recordbuilders;

public class JoinCondition {
	private String left;
	private String right;
	private String condition;

	/**
	 * Create an empty join condition
	 */
	public JoinCondition() {
		super();
	}

	/**
	 * A join condition consists of a left and right side and a condition operator.
	 * Note that all strings are taken literally, so they can be field names, expressions or constants.
	 *
	 * @param left
	 * @param right
	 * @param condition is a SQL operator like =, <, >, <=, >=, <>
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

	public void setLeft(String left) {
		this.left = left;
	}

	public void setRight(String right) {
		this.right = right;
	}

	public void setCondition(String condition) {
		this.condition = condition;
	}


}
