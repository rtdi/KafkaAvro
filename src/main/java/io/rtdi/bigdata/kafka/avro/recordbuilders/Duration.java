package io.rtdi.bigdata.kafka.avro.recordbuilders;

/**
 * Class to represent a duration in a certain time unit
 */
public class Duration {
	private int value;
	private TimeUnit unit;

	/**
	 * Constructor
	 *
	 * @param value the value
	 * @param unit the time unit
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
	public int getValue() {
		return value;
	}

	/**
	 * Get the time unit
	 *
	 * @return the time unit
	 */
	public TimeUnit getUnit() {
		return unit;
	}
}
