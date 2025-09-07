package io.rtdi.bigdata.kafka.avro.recordbuilders;

public enum TimeUnit {
	HOURS(1),
	WEEKS(2),
	MONTHS(3),
	QUARTERS(4),
	YEARS(5);

	private int index;

	TimeUnit(int index) {
		this.index = index;
	}

	public int getIndex() {
		return index;
	}
}
