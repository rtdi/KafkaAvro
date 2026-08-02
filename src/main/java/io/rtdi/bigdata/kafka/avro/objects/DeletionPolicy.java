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
	 * Creates a deletion policy with a numeric amount, a time unit, and a descriptive note.
	 *
	 * @param value the number of units in the policy
	 * @param unit the time unit for the policy period
	 * @param description a free-form description of the policy
	 */
	public DeletionPolicy(int value, TimeUnit unit, String description) {
		this.value = value;
		this.unit = unit;
		this.description = description;
	}

	/**
	 * Gets the numeric amount specified by the deletion policy.
	 *
	 * @return the number of units
	 */
	public int getValue() {
		return value;
	}

	/**
	 * Gets the time unit associated with the deletion policy.
	 *
	 * @return the time unit
	 */
	public TimeUnit getUnit() {
		return unit;
	}

	/**
	 * Gets the free-form description of the deletion policy.
	 *
	 * @return the policy description
	 */
	public String getDescription() {
		return description;
	}

	/**
	 * Compares this deletion policy to another object for equality.
	 *
	 * @param o the object to compare against
	 * @return {@code true} when the two policies are equivalent
	 */
	@Override
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

	/**
	 * Returns a stable hash code for the deletion policy.
	 *
	 * @return the hash code for this instance
	 */
	@Override
	public int hashCode() {
		return 1;
	}

}
