package io.rtdi.bigdata.kafka.avro.objects;

import io.rtdi.bigdata.kafka.avro.AvroUtils;

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
	/**
	 * Creates a new instance of this class.
	 * @param value the parameter value
	 * @param unit the parameter value
	 * @param description the parameter value
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
	/**
	 * Executes the int getValue operation.
	 */
	public int getValue() {
		return value;
	}

	/**
	 * Get the unit of time
	 * @return unit of time
	 */
	/**
	 * Executes the TimeUnit getUnit operation.
	 */
	public TimeUnit getUnit() {
		return unit;
	}

	/**
	 * Get the description of the policy
	 *
	 * @return free form description of the policy
	 */
	/**
	 * Executes the String getDescription operation.
	 */
	public String getDescription() {
		return description;
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
		if (o == null || !(o instanceof DeletionPolicy)) {
			return false;
		}
		DeletionPolicy other = (DeletionPolicy) o;
		if (!AvroUtils.isEqual(this.value, other.value)) {
			return false;
		}
		if (!AvroUtils.isEqual(this.unit, other.unit)) {
			return false;
		}
		if (!AvroUtils.isEqual(this.description, other.description)) {
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
