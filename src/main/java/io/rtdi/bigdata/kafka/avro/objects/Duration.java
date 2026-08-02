package io.rtdi.bigdata.kafka.avro.objects;

import io.rtdi.bigdata.kafka.avro.AvroUtils;

/**
 * Class to represent a duration in a certain time unit
 */
public class Duration {
	private int value;
	private TimeUnit unit;

	/**
	 * Creates an empty duration instance.
	 */
	public Duration() {
	}

	/**
	 * Creates a duration with the given numeric value and time unit.
	 *
	 * @param value the duration value
	 * @param unit the time unit for the duration
	 */
	public Duration(int value, TimeUnit unit) {
		super();
		this.value = value;
		this.unit = unit;
	}

	/**
	 * Gets the numeric duration value.
	 *
	 * @return the duration value
	 */
	public int getValue() {
		return value;
	}

	/**
	 * Gets the time unit associated with the duration.
	 *
	 * @return the time unit
	 */
	public TimeUnit getUnit() {
		return unit;
	}

	/**
	 * Compares this duration to another object for equality.
	 *
	 * @param o the object to compare against
	 * @return {@code true} when the two durations are equivalent
	 */
	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || !(o instanceof Duration)) {
			return false;
		}
		Duration other = (Duration) o;
		if (!AvroUtils.isEqual(this.value, other.value)) {
			return false;
		}
		if (!AvroUtils.isEqual(this.unit, other.unit)) {
			return false;
		}
		return true;
	}

	/**
	 * Returns a stable hash code for the duration.
	 *
	 * @return the hash code for this instance
	 */
	@Override
	public int hashCode() {
		return 1;
	}

}
