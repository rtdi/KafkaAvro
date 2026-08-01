package io.rtdi.bigdata.kafka.avro.objects;

/**
 * Times units for retention periods and similar
 */
public enum TimeUnit {
	/**
	 * Hours
	 */
	HOURS(1),
	/**
	 * Weeks
	 */
	WEEKS(2),
	/**
	 * Months
	 */
	MONTHS(3),
	/**
	 * Quarters
	 */
	QUARTERS(4),
	/**
	 * Years
	 */
	YEARS(5);

	private int index;

	TimeUnit(int index) {
		this.index = index;
	}

	/**
	 * Get the index of the time unit
	 *
	 * @return the index of the time unit
	 */
	/**
	 * Executes the int getIndex operation.
	 */
	public int getIndex() {
		return index;
	}
}
