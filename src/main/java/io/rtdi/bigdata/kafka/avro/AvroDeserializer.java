package io.rtdi.bigdata.kafka.avro;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

import io.rtdi.bigdata.kafka.avro.datatypes.LogicalDataTypesRegistry;

/**
 * This is class does deserialize Avro records. It uses the same format as Kafka itself, hence
 * even if data is serialized by others, e.g. Kafka Connect, it can be consumed.
 *
 */
public class AvroDeserializer {

	private static final DecoderFactory decoderFactory = DecoderFactory.get();
	/**
	 * The decoder to be reused.
	 */
	protected BinaryDecoder decoder;

	static {
		LogicalDataTypesRegistry.registerAll();
	}

	/**
	 * Takes the Kafka message payload and extract the schemaid from it. Based on that the schema can be read from the schema registry.
	 *
	 * @param data Kafka message payload in binary form
	 * @return schemaid associated with that message
	 * @throws IOException in case this is not a valid Avro Kafka message
	 */
	public static int getSchemaId(byte[] data) throws IOException {
		if (data != null) {
			if (data[0] != AvroUtils.MAGIC_BYTE) {
				throw new IOException("Not a valid Kafka Avro message frame");
			} else {
				ByteBuffer bb = ByteBuffer.wrap(data, 1, Integer.BYTES);
				return bb.getInt();
			}
		} else {
			return -1;
		}
	}

	/**
	 * Converts a byte[] into an Avro GenericRecord using the supplied schema.
	 * The schema must be read from the schema registry using the message's schema id, see {@link #getSchemaId(byte[])}
	 *
	 * This clas is not thread safe as it is reusing the Avro decoder.
	 *
	 * @param data with the binary Avro representation
	 * @param schema used for the deserialization
	 * @return AvroRecord in Jexl abstraction
	 * @throws IOException in case this is not a valid Avro Kafka message
	 */
	public GenericRecord deserialize(byte[] data, Schema schema) throws IOException {
		if (data != null) {
			try (ByteArrayInputStream in = new ByteArrayInputStream(data); ) {
				int b = in.read();
				if (b != AvroUtils.MAGIC_BYTE) {
					throw new IOException("Not a valid Kafka Avro message frame");
				} else {
					in.skip(Integer.BYTES);
					decoder = decoderFactory.directBinaryDecoder(in, decoder);
					DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
					return reader.read(null, decoder);
				}
			}
		} else {
			return null;
		}
	}
}
