package io.rtdi.bigdata.kafka.avro;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.rtdi.bigdata.kafka.avro.datatypes.AvroAnyPrimitive;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroArray;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroBoolean;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroByte;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroBytes;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroCLOB;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroDate;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroDecimal;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroDouble;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroEnum;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroFixed;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroFloat;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroInt;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroLocalTimestamp;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroLocalTimestampMicros;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroLong;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroMap;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroNCLOB;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroNVarchar;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroSTGeometry;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroSTPoint;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroShort;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroString;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroTime;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroTimeMicros;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroTimestamp;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroTimestampMicros;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroType;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroUUID;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroUri;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroVarchar;
import io.rtdi.bigdata.kafka.avro.datatypes.LogicalDataTypesRegistry;
import io.rtdi.bigdata.kafka.avro.recordbuilders.AvroRecordArray;
import io.rtdi.bigdata.kafka.avro.recordbuilders.AvroRecordField;
import io.rtdi.bigdata.kafka.avro.recordbuilders.SchemaBuilder;

/**
 * Some tests for data conversions
 */
public class ValueSetterTest {
	private static final String OTHER = "other";
	private static final String COL_ANY_PRIMITIVE = "COL_AnyPrimitive";
	private static final String COL_UUID = "COL_UUID";
	private static final String COL_URI = "COL_Uri";
	private static final String COL_FIXED = "COL_Fixed";
	private static final String COL_BYTES = "COL_Bytes";
	private static final String COL_BOOLEAN = "COL_Boolean";
	private static final String COMPLEX = "complex";
	private static final String COL_ARRAY = "COL_Array";
	private static final String COL_MAP = "COL_Map";
	private static final String COL_ENUM = "COL_Enum";
	private static final String SPATIAL = "spatial";
	private static final String COL_ST_POINT = "COL_STPoint";
	private static final String COL_ST_GEOMETRY = "COL_STGeometry";
	private static final String DATE = "date";
	private static final String COL_TIMESTAMP_MICROS = "COL_TimestampMicros";
	private static final String COL_TIMESTAMP = "COL_Timestamp";
	private static final String COL_LOCALTIMESTAMP_MICROS = "COL_LocalTimestampMicros";
	private static final String COL_LOCALTIMESTAMP = "COL_LocalTimestamp";
	private static final String COL_TIME_MICROS = "COL_TimeMicros";
	private static final String COL_TIME = "COL_Time";
	private static final String COL_DATE = "COL_Date";
	private static final String TEXT = "text";
	private static final String COL_VARCHAR = "COL_Varchar";
	private static final String COL_STRING = "COL_String";
	private static final String COL_NVARCHAR = "COL_NVarchar";
	private static final String COL_NCLOB = "COL_NCLOB";
	private static final String COL_CLOB = "COL_CLOB";
	private static final String NUMBERS = "numbers";
	private static final String COL_SHORT = "COL_Short";
	private static final String COL_LONG = "COL_Long";
	private static final String COL_FLOAT = "COL_Float";
	private static final String COL_DOUBLE = "COL_Double";
	private static final String COL_DECIMAL = "COL_Decimal";
	private static final String COL_BYTE = "COL_Byte";
	private static final String COL_INT = "COL_Int";
	private static final String OTHER_SCHEMA = "OtherSchema";
	private static final String COMPLEX_SCHEMA = "ComplexSchema";
	private static final String SPATIAL_SCHEMA = "SpatialSchema";
	private static final String DATE_SCHEMA = "DateSchema";
	private static final String TEXT_SCHEMA = "TextSchema";
	private static final String NUMBERS_SCHEMA = "NumbersSchema";
	private static EncoderFactory encoderFactory = EncoderFactory.get();
	private static final DecoderFactory decoderFactory = DecoderFactory.get();
	private static Conversion<BigDecimal> decimalconversion = new Conversions.DecimalConversion();
	private static Instant nowinstant = Instant.now();
	private static LocalDate nowlocaldate = LocalDate.ofInstant(nowinstant, ZoneId.of("UTC"));
	private static LocalTime nowlocaltime = LocalTime.ofInstant(nowinstant, ZoneId.of("UTC"));

	/**
	 * @throws Exception if something goes wrong
	 */
	@BeforeAll
	public static void setUp() throws Exception {
	}

	/**
	 * @throws Exception if something goes wrong
	 */
	@AfterAll
	public static void tearDown() throws Exception {
	}

	/**
	 * Tests
	 */
	@Test
	public void test() {
		try {
			LogicalDataTypesRegistry.registerAll();
			Schema schema = buildAllDataTypesSchema();
			GenericRecord recordout = createNewRecordAvroWay(schema);
			byte[] bytes = serialize(recordout);
			GenericRecord recordin = deserialize(schema, bytes);
			assertEquals(recordout, recordin, "Avro-native: Data before and after serialize/deserialize is different");
			GenericRecord recordout2 = createNewRecordViaLogicalDataTypes(schema);
			assertEquals(recordout, recordout2, "AvroType: The record set via the helper methods is different to the Avro-native way");
			bytes = serialize(recordout2);
			GenericRecord recordin2 = deserialize(schema, bytes);
			assertEquals(recordout, recordin2, "AvroType: Data before and after serialize/deserialize is different");

			GenericRecord numbers = AvroType.getSubRecord(recordin2, NUMBERS);
			assertNotNull(numbers, "NUMBERS field is empty");
			assertEquals(1, AvroType.getRecordFieldValue(numbers, COL_INT));

			GenericRecord dates = AvroType.getSubRecord(recordin2, DATE);
			assertNotNull(dates, "DATE field is empty");
			assertEquals(nowlocaldate, AvroType.getRecordFieldValue(dates, COL_DATE));

			List<GenericRecord> others = AvroType.getSubRecordArray(recordin2, OTHER);
			assertNotNull(others, "OTHER field is empty");
			assertEquals("urn:none", AvroType.getRecordFieldValue(others.get(0), COL_URI).toString());

		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	private static GenericRecord deserialize(Schema schema, byte[] bytes) throws IOException {
		GenericRecord recordin;
		try (ByteArrayInputStream in = new ByteArrayInputStream(bytes); ) {
			BinaryDecoder decoder = decoderFactory.directBinaryDecoder(in, null);
			DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
			recordin = reader.read(null, decoder);
		}
		return recordin;
	}

	private static byte[] serialize(GenericRecord recordout) throws IOException {
		byte[] bytes;
		try ( ByteArrayOutputStream out = new ByteArrayOutputStream(); ) {
			BinaryEncoder encoder = encoderFactory.directBinaryEncoder(out, null);
			DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(recordout.getSchema());
			writer.write(recordout, encoder);
			encoder.flush();
			bytes = out.toByteArray();
		}
		return bytes;
	}

	private static GenericRecord createNewRecordViaLogicalDataTypes(Schema schema) {
		GenericRecord recordout2 = new GenericData.Record(schema);
		GenericRecord recordNumbers = AvroType.createChildRecordFor(recordout2, NUMBERS);
		AvroType.putRecordValue(recordNumbers, COL_INT, 1);
		AvroType.putRecordValue(recordNumbers, COL_BYTE, 1);
		AvroType.putRecordValue(recordNumbers, COL_DECIMAL, 1.0);
		AvroType.putRecordValue(recordNumbers, COL_DOUBLE, 1.0);
		AvroType.putRecordValue(recordNumbers, COL_FLOAT, 2.0f);
		AvroType.putRecordValue(recordNumbers, COL_LONG, 1L);
		AvroType.putRecordValue(recordNumbers, COL_SHORT, 1);
		GenericRecord recordText = AvroType.createChildRecordFor(recordout2, TEXT);
		AvroType.putRecordValue(recordText, COL_CLOB, "Hello CLOB");
		AvroType.putRecordValue(recordText, COL_NCLOB, "Hello NCLOB");
		AvroType.putRecordValue(recordText, COL_NVARCHAR, "Hello NVarchar");
		AvroType.putRecordValue(recordText, COL_STRING, "Hello String");
		AvroType.putRecordValue(recordText, COL_VARCHAR, "Hello Varchar");
		GenericRecord recordDate = AvroType.createChildRecordFor(recordout2, DATE);
		AvroType.putRecordValue(recordDate, COL_DATE, nowlocaldate);
		AvroType.putRecordValue(recordDate, COL_TIME, nowlocaltime);
		AvroType.putRecordValue(recordDate, COL_TIME_MICROS, nowlocaltime);
		AvroType.putRecordValue(recordDate, COL_TIMESTAMP, nowinstant);
		AvroType.putRecordValue(recordDate, COL_TIMESTAMP_MICROS, nowinstant);
		AvroType.putRecordValue(recordDate, COL_LOCALTIMESTAMP, nowinstant);
		AvroType.putRecordValue(recordDate, COL_LOCALTIMESTAMP_MICROS, nowinstant);
		GenericRecord recordSpatial = AvroType.createChildRecordFor(recordout2, SPATIAL);
		AvroType.putRecordValue(recordSpatial, COL_ST_GEOMETRY, "Hello STGeometry");
		AvroType.putRecordValue(recordSpatial, COL_ST_POINT, "Hello STPoint");
		GenericRecord recordComplex = AvroType.createChildRecordFor(recordout2, COMPLEX);
		AvroType.putRecordValue(recordComplex, COL_ENUM, RowType.INSERT);
		HashMap<CharSequence, Long> hm = new HashMap<>();
		hm.put(new Utf8("Key1"), 99L);
		AvroType.putRecordValue(recordComplex, COL_ENUM, RowType.INSERT);
		AvroType.putRecordValue(recordComplex, COL_MAP, hm);
		AvroType.putRecordValue(recordComplex, COL_ARRAY, new Integer[] {55});
		GenericRecord recordOther = AvroType.addChildToArrayOfRecords(recordout2, OTHER);
		AvroType.putRecordValue(recordOther, COL_BOOLEAN, true);
		AvroType.putRecordValue(recordOther, COL_BYTES, "Hello Bytes".getBytes());
		AvroType.putRecordValue(recordOther, COL_FIXED, new byte[5]);
		AvroType.putRecordValue(recordOther, COL_URI, "urn:none");
		AvroType.putRecordValue(recordOther, COL_UUID, "1234-5678");
		AvroType.putRecordValue(recordOther, COL_ANY_PRIMITIVE, 1234);
		return recordout2;
	}

	private static GenericRecord createNewRecordAvroWay(Schema schema) {
		GenericRecord recordout = new GenericData.Record(schema);
		GenericRecord recordNumbers = new GenericData.Record(AvroUtils.getBaseSchema(schema.getField(NUMBERS).schema()));
		recordNumbers.put(COL_INT, 1);
		recordNumbers.put(COL_BYTE, 1);
		Schema decimalschema = AvroUtils.getBaseSchema(recordNumbers.getSchema().getField(COL_DECIMAL).schema());
		recordNumbers.put(COL_DECIMAL, decimalconversion.toBytes(BigDecimal.valueOf(1.0), decimalschema, decimalschema.getLogicalType()));
		recordNumbers.put(COL_DOUBLE, Double.valueOf(1.0));
		recordNumbers.put(COL_FLOAT, Float.valueOf(2.0f));
		recordNumbers.put(COL_LONG, 1L);
		recordNumbers.put(COL_SHORT, 1);
		recordout.put(NUMBERS, recordNumbers);
		GenericRecord recordText = new GenericData.Record(AvroUtils.getBaseSchema(schema.getField(TEXT).schema()));
		recordText.put(COL_CLOB, "Hello CLOB");
		recordText.put(COL_NCLOB, "Hello NCLOB");
		recordText.put(COL_NVARCHAR, "Hello NVarchar");
		recordText.put(COL_STRING, "Hello String");
		recordText.put(COL_VARCHAR, "Hello Varchar");
		recordout.put(TEXT, recordText);
		GenericRecord recordDates = new GenericData.Record(AvroUtils.getBaseSchema(schema.getField(DATE).schema()));
		recordDates.put(COL_DATE, (int) nowlocaldate.toEpochDay());
		recordDates.put(COL_TIME, (int) (nowlocaltime.toNanoOfDay()/1000000L));
		recordDates.put(COL_TIME_MICROS, nowlocaltime.toNanoOfDay()/1000L);
		recordDates.put(COL_TIMESTAMP, nowinstant.toEpochMilli());
		recordDates.put(COL_TIMESTAMP_MICROS, nowinstant.getEpochSecond() * 1000000L + nowinstant.getNano() / 1000L);
		recordDates.put(COL_LOCALTIMESTAMP, nowinstant.toEpochMilli());
		recordDates.put(COL_LOCALTIMESTAMP_MICROS, nowinstant.getEpochSecond() * 1000000L + nowinstant.getNano() / 1000L);
		recordout.put(DATE, recordDates);
		GenericRecord recordSpatial = new GenericData.Record(AvroUtils.getBaseSchema(schema.getField(SPATIAL).schema()));
		recordSpatial.put(COL_ST_GEOMETRY, "Hello STGeometry");
		recordSpatial.put(COL_ST_POINT, "Hello STPoint");
		recordout.put(SPATIAL, recordSpatial);
		GenericRecord recordComplex = new GenericData.Record(AvroUtils.getBaseSchema(schema.getField(COMPLEX).schema()));
		recordComplex.put(COL_ENUM, new GenericData.EnumSymbol(AvroUtils.getBaseSchema(recordComplex.getSchema().getField(COL_ENUM).schema()), RowType.INSERT.name()));
		HashMap<CharSequence, Long> hm = new HashMap<>();
		hm.put(new Utf8("Key1"), 99L);
		recordComplex.put(COL_MAP, hm);
		ArrayList<Integer> l = new ArrayList<>();
		l.add(55);
		recordComplex.put(COL_ARRAY, l);
		recordout.put(COMPLEX, recordComplex);
		GenericRecord recordOther = new GenericData.Record(AvroUtils.getBaseSchema(schema.getField(OTHER).schema()).getElementType());
		recordOther.put(COL_BOOLEAN, true);
		recordOther.put(COL_BYTES, ByteBuffer.wrap("Hello Bytes".getBytes()));
		recordOther.put(COL_FIXED, new GenericData.Fixed(AvroUtils.getBaseSchema(recordOther.getSchema().getField(COL_FIXED).schema()), new byte[5]));
		recordOther.put(COL_URI, "urn:none");
		recordOther.put(COL_UUID, "1234-5678");
		recordOther.put(COL_ANY_PRIMITIVE, 1234);
		List<GenericRecord> records = new ArrayList<>();
		records.add(recordOther);
		recordout.put(OTHER, records);
		return recordout;
	}

	private static Schema buildAllDataTypesSchema() {
		SchemaBuilder builder = new SchemaBuilder("Schema1", null);

		AvroRecordField builderNumbers = builder.addColumnRecord(NUMBERS, null, true, NUMBERS_SCHEMA, null);
		builderNumbers.add(COL_INT, AvroInt.getSchema(), null, true);
		builderNumbers.add(COL_BYTE, AvroByte.getSchema(), null, true);
		builderNumbers.add(COL_DECIMAL, AvroDecimal.getSchema(20,7), null, true);
		builderNumbers.add(COL_DOUBLE, AvroDouble.getSchema(), null, true);
		builderNumbers.add(COL_FLOAT, AvroFloat.getSchema(), null, true);
		builderNumbers.add(COL_LONG, AvroLong.getSchema(), null, true);
		builderNumbers.add(COL_SHORT, AvroShort.getSchema(), null, true);
		AvroRecordField builderText = builder.addColumnRecord(TEXT, null, true, TEXT_SCHEMA, null);
		builderText.add(COL_CLOB, AvroCLOB.getSchema(), null, true);
		builderText.add(COL_NCLOB, AvroNCLOB.getSchema(), null, true);
		builderText.add(COL_NVARCHAR, AvroNVarchar.getSchema(20), null, true);
		builderText.add(COL_STRING, AvroString.getSchema(), null, true);
		builderText.add(COL_VARCHAR, AvroVarchar.getSchema(30), null, true);
		AvroRecordField builderDate = builder.addColumnRecord(DATE, null, true, DATE_SCHEMA, null);
		builderDate.add(COL_DATE, AvroDate.getSchema(), null, true);
		builderDate.add(COL_TIME, AvroTime.getSchema(), null, true);
		builderDate.add(COL_TIME_MICROS, AvroTimeMicros.getSchema(), null, true);
		builderDate.add(COL_TIMESTAMP, AvroTimestamp.getSchema(), null, true);
		builderDate.add(COL_TIMESTAMP_MICROS, AvroTimestampMicros.getSchema(), null, true);
		builderDate.add(COL_LOCALTIMESTAMP, AvroLocalTimestamp.getSchema(), null, true);
		builderDate.add(COL_LOCALTIMESTAMP_MICROS, AvroLocalTimestampMicros.getSchema(), null, true);
		AvroRecordField builderSpatial = builder.addColumnRecord(SPATIAL, null, true, SPATIAL_SCHEMA, null);
		builderSpatial.add(COL_ST_GEOMETRY, AvroSTGeometry.getSchema(), null, true);
		builderSpatial.add(COL_ST_POINT, AvroSTPoint.getSchema(), null, true);
		AvroRecordField builderComplex = builder.addColumnRecord(COMPLEX, null, true, COMPLEX_SCHEMA, null);
		builderComplex.add(COL_ENUM, AvroEnum.getSchema(RowType.class), null, true);
		builderComplex.add(COL_MAP, AvroMap.getSchema(AvroLong.getSchema()), null, true);
		builderComplex.add(COL_ARRAY, AvroArray.getSchema(AvroInt.getSchema()), null, true);
		AvroRecordArray builderOther = builder.addColumnRecordArray(OTHER, null, OTHER_SCHEMA, null);
		builderOther.add(COL_BOOLEAN, AvroBoolean.getSchema(), null, true);
		builderOther.add(COL_BYTES, AvroBytes.getSchema(), null, true);
		builderOther.add(COL_FIXED, AvroFixed.getSchema(5), null, true);
		builderOther.add(COL_URI, AvroUri.getSchema(), null, true);
		builderOther.add(COL_UUID, AvroUUID.getSchema(), null, true);
		builderOther.add(COL_ANY_PRIMITIVE, AvroAnyPrimitive.getSchema(), null, true);
		builder.build();
		Schema schema = builder.getSchema();
		return schema;
	}

}
