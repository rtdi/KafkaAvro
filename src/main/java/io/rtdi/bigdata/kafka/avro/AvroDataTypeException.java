package io.rtdi.bigdata.kafka.avro;

import org.apache.avro.AvroRuntimeException;

/**
 * When the data type is not as expected this exception is thrown.
 */
public class AvroDataTypeException extends AvroRuntimeException {

	private static final long serialVersionUID = -6100175607149184313L;

	/**
	 * @param message to be shown
	 */
	public AvroDataTypeException(String message) {
		super(message);
	}

}
