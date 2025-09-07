package io.rtdi.bigdata.kafka.avro.recordbuilders;

public class Duration {
	private int value;
	private TimeUnit unit;

	public Duration(int value, TimeUnit unit) {
		super();
		this.value = value;
		this.unit = unit;
	}

	public int getValue() {
		return value;
	}

	public TimeUnit getUnit() {
		return unit;
	}
}
