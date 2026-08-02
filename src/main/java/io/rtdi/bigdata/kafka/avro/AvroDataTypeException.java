package io.rtdi.bigdata.kafka.avro;

import org.apache.avro.AvroRuntimeException;

/**
 * When the data type is not as expected this exception is thrown.
 */
public class AvroDataTypeException extends AvroRuntimeException {

	private static final long serialVersionUID = -6100175607149184313L;

	/**
	 * Creates a new instance of this class.
	 * @param message the parameter value
	 */
	public AvroDataTypeException(String message) {
		super(message);
	}

	/**
	 * Creates a new instance of this class.
	 * @param message the parameter value
	 * @param e the causing exception
	 */
	public AvroDataTypeException(String message, Exception e) {
		super(message, e);
	}

}
