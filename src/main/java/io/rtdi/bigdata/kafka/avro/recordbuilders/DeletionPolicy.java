package io.rtdi.bigdata.kafka.avro.recordbuilders;

/**
 * Information about a deletion policy for data
 */
public class DeletionPolicy {
	private int value;
	private TimeUnit unit;
	private String description;

	/**
	 * Create a deletion policy
	 *
	 * @param value number of units
	 * @param unit unit of time
	 * @param description free form description of the policy
	 */
	public DeletionPolicy(int value, TimeUnit unit, String description) {
		this.value = value;
		this.unit = unit;
		this.description = description;
	}

	/**
	 * Get the number of units
	 *
	 * @return number of units
	 */
	public int getValue() {
		return value;
	}

	/**
	 * Get the unit of time
	 * @return unit of time
	 */
	public TimeUnit getUnit() {
		return unit;
	}

	/**
	 * Get the description of the policy
	 *
	 * @return free form description of the policy
	 */
	public String getDescription() {
		return description;
	}

}
