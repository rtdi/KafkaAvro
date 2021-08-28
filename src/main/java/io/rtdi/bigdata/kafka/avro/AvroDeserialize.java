package io.rtdi.bigdata.kafka.avro;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.AvroRuntimeException;
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
public class AvroDeserialize {

	private static final DecoderFactory decoderFactory = DecoderFactory.get();
	
	static {
		LogicalDataTypesRegistry.registerAll();
	}
	
	public static int getSchemaId(byte[] data) throws IOException {
		if (data != null) {
			ByteBuffer schemaid = ByteBuffer.allocate(Integer.BYTES);
			try (ByteArrayInputStream in = new ByteArrayInputStream(data); ) {
				int b = in.read();
				if (b != AvroUtils.MAGIC_BYTE) {
					throw new AvroRuntimeException("Not a valid Kafka message frame");
				} else {
					in.read(schemaid.array());
					return schemaid.getInt();
				}
			}
		} else {
			return -1;
		}
	}

	/**
	 * Converts a byte[] into an Avro GenericRecord using the supplied schema.
	 * The schema must be read from the schema registry using the message's schema id, see {@link #getSchemaId(byte[])}
	 * 
	 * @param data with the binary Avro representation
	 * @param schema used for the deserialization
	 * @return AvroRecord in Jexl abstraction
	 * @throws IOException In case anything went wrong
	 */
	public static GenericRecord deserialize(byte[] data, Schema schema) throws IOException {
		if (data != null) {
			ByteBuffer schemaidb = ByteBuffer.allocate(Integer.BYTES);
			try (ByteArrayInputStream in = new ByteArrayInputStream(data); ) {
				int b = in.read();
				if (b != AvroUtils.MAGIC_BYTE) {
					throw new AvroRuntimeException("Not a valid Kafka message frame");
				} else {
					in.read(schemaidb.array());
					BinaryDecoder decoder = decoderFactory.directBinaryDecoder(in, null);
					DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
					return reader.read(null, decoder);
				}
			}
		} else {
			return null;
		}
	}
}
