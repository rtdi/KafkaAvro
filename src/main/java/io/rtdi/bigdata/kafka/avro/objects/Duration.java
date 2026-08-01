package io.rtdi.bigdata.kafka.avro.objects;

import io.rtdi.bigdata.kafka.avro.AvroUtils;

/**
 * Class to represent a duration in a certain time unit
 */
public class Duration {
	private int value;
	private TimeUnit unit;

	/**
	 * Creates a new instance of this class.
	 */
	public Duration() {
	}

	/**
	 * Constructor
	 *
	 * @param value the value
	 * @param unit the time unit
	 */
	/**
	 * Creates a new instance of this class.
	 * @param value the parameter value
	 * @param unit the parameter value
	 */
	public Duration(int value, TimeUnit unit) {
		super();
		this.value = value;
		this.unit = unit;
	}

	/**
	 * Get the value
	 * @return the value
	 */
	/**
	 * Executes the int getValue operation.
	 */
	public int getValue() {
		return value;
	}

	/**
	 * Get the time unit
	 *
	 * @return the time unit
	 */
	/**
	 * Executes the TimeUnit getUnit operation.
	 */
	public TimeUnit getUnit() {
		return unit;
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

	@Override
	/**
	 * Executes the int hashCode operation.
	 */
	public int hashCode() {
		return 1;
	}

}
