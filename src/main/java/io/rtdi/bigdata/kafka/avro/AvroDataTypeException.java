package io.rtdi.bigdata.kafka.avro;

import org.apache.avro.AvroRuntimeException;

public class AvroDataTypeException extends AvroRuntimeException {

	private static final long serialVersionUID = -6100175607149184313L;

	/**
	 * @param message to be shown
	 */
	public AvroDataTypeException(String message) {
		super(message);
	}

}
