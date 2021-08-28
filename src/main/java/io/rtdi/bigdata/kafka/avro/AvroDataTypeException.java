package io.rtdi.bigdata.kafka.avro;

import org.apache.avro.AvroRuntimeException;

public class AvroDataTypeException extends AvroRuntimeException {

	private static final long serialVersionUID = -6210537140205590102L;

	public AvroDataTypeException(String message) {
		super(message);
	}

}
