package io.rtdi.bigdata.kafka.avro.datatypes;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

import io.rtdi.bigdata.kafka.avro.AvroDataTypeException;

public interface IAvroDatatype {

	void toString(StringBuffer b, Object value);

	Object convertToInternal(Object value) throws AvroDataTypeException;

	Type getBackingType();

	Schema getDatatypeSchema();

	AvroType getAvroType();
	
	Object convertToJava(Object value) throws AvroDataTypeException;
}
