package io.rtdi.bigdata.kafka.avro.recordbuilders;

public class DeletionPolicy {
	private int value;
	private TimeUnit unit;
	private String description;

	public DeletionPolicy(int value, TimeUnit unit, String description) {
		this.value = value;
		this.unit = unit;
		this.description = description;
	}

	public int getValue() {
		return value;
	}

	public TimeUnit getUnit() {
		return unit;
	}

	public String getDescription() {
		return description;
	}

}
