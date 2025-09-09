package io.rtdi.bigdata.kafka.avro;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import io.rtdi.bigdata.kafka.avro.datatypes.*;

/**
 * some tests to verify the conversion options of the data types
 */
public class ConversionOptionsTest {
	private static Instant nowinstant = Instant.now();
	private static LocalDate nowlocaldate = LocalDate.ofInstant(nowinstant, ZoneId.of("UTC"));
	private static LocalTime nowlocaltime = LocalTime.ofInstant(nowinstant, ZoneId.of("UTC"));

	/**
	 * @throws Exception something went wrong
	 */
	@BeforeAll
	public static void setUp() throws Exception {
	}

	/**
	 * @throws Exception something went wrong
	 */
	@AfterAll
	public static void tearDown() throws Exception {
	}

	/**
	 * Test method
	 */
	@Test
	public void test() {
		try {
			/*
				| AvroType    | Supported Java types         | Note |
				|-------------|------------------------------|------|
				| AvroInt     | Integer, String, Number      |      |
				| AvroByte    | Integer, String, Number      | value must be in Java Byte range |
				| AvroDecimal | BigDecimal, String, Number   |      |
				| AvroDouble  | Double, String, Number       |      |
				| AvroFloat   | Float, String, Number        |      |
				| AvroLong    | Long, String, Number         |      |
				| AvroShort   | Integer, String, Number      | value must be in Java Short range |
			 */
			AvroInt i = AvroInt.create();
			assertEquals(Integer.valueOf(23),
					i.convertToJava(i.convertToInternal(23)),
					"AvroInt conversion failed with Integer");
			assertEquals(Integer.valueOf(23),
					i.convertToJava(i.convertToInternal("23")),
					"AvroInt conversion failed with String");
			assertEquals(Integer.valueOf(23),
					i.convertToJava(i.convertToInternal(23.7f)),
					"AvroInt conversion failed with Float");

			AvroDecimal d = AvroDecimal.create(20, 7);
			assertEquals(BigDecimal.valueOf(239000000, 7),
					d.convertToJava(d.convertToInternal(BigDecimal.valueOf(23.9d))),
					"AvroDecimal conversion failed with BigDecimal");
			assertEquals(BigDecimal.valueOf(239000000, 7),
					d.convertToJava(d.convertToInternal("23.9")),
					"AvroDecimal conversion failed with String");
			assertEquals(BigDecimal.valueOf(239000000, 7),
					d.convertToJava(d.convertToInternal(23.9f)),
					"AvroDecimal conversion failed with Float");

			AvroDouble dd = AvroDouble.create();
			assertEquals(Double.valueOf(23.9),
					dd.convertToJava(dd.convertToInternal(23.9)),
					"AvroDouble conversion failed with Double");
			assertEquals(Double.valueOf(23.9),
					dd.convertToJava(dd.convertToInternal("23.9")),
					"AvroDouble conversion failed with String");
			assertEquals(Double.valueOf(23.9),
					dd.convertToJava(dd.convertToInternal(23.9f)),
					"AvroDouble conversion failed with Float");

			AvroFloat f = AvroFloat.create();
			assertEquals(Float.valueOf(23.9f),
					f.convertToJava(f.convertToInternal(23.9f)),
					"AvroFloat conversion failed with Float");
			assertEquals(Float.valueOf(23.9f),
					f.convertToJava(f.convertToInternal("23.9")),
					"AvroFloat conversion failed with String");
			assertEquals(Float.valueOf(23.9f),
					f.convertToJava(f.convertToInternal(23.9)),
					"AvroFloat conversion failed with Double");

			AvroLong l = AvroLong.create();
			assertEquals(Long.valueOf(-45L),
					l.convertToJava(l.convertToInternal(-45L)),
					"AvroLong conversion failed with Long");
			assertEquals(Long.valueOf(-45L),
					l.convertToJava(l.convertToInternal("-45")),
					"AvroLong conversion failed with String");
			assertEquals(Long.valueOf(-45L),
					l.convertToJava(l.convertToInternal(-45.0)),
					"AvroLong conversion failed with Double");

			AvroShort s = AvroShort.create();
			assertEquals(Short.valueOf((short) 23),
					s.convertToJava(s.convertToInternal((short) 23)),
					"AvroShort conversion failed with Short");
			assertEquals(Short.valueOf((short) 23),
					s.convertToJava(s.convertToInternal("23")),
					"AvroShort conversion failed with String");
			assertEquals(Short.valueOf((short) 23),
					s.convertToJava(s.convertToInternal(23.0)),
					"AvroShort conversion failed with Double");

			/*
				| AvroType     | Supported Java types         |      |
				|--------------|------------------------------|------|
				| AvroCLOB     | CharSequence, Object         | error in case non-ASCII chars are found     |
				| AvroNCLOB    | CharSequence, Object         |      |
				| AvroNVarchar | CharSequence, Object         | longer texts are truncated     |
				| AvroString   | CharSequence, Object         |      |
				| AvroVarchar  | CharSequence, Object         | longer texts are truncated, error in case non-ASCII chars are found |
			 */
			AvroCLOB clob = AvroCLOB.create();
			assertEquals("Long Text", clob.convertToJava(clob.convertToInternal("Long Text")), "AvroCLOB conversion failed with String");
			assertThrows(
					AvroDataTypeException.class,
					() -> { clob.convertToJava(clob.convertToInternal("ö")); },
					"AvroCLOB conversion does nto throw exception on non-ASCII chars");

			AvroNCLOB nclob = AvroNCLOB.create();
			assertEquals("Long Text",
					nclob.convertToJava(nclob.convertToInternal("Long Text")),
					"AvroNCLOB conversion failed with String");

			AvroNVarchar nvarchar = AvroNVarchar.create(3);
			assertEquals("Lon",
					nvarchar.convertToJava(nvarchar.convertToInternal("Long Text")),
					"AvroNVarchar conversion failed with String");

			AvroString astring = AvroString.create();
			assertEquals("Long Text",
					astring.convertToJava(astring.convertToInternal("Long Text")),
					"AvroString conversion failed with String");

			AvroVarchar varchar = AvroVarchar.create(3);
			assertEquals("Lon",
					varchar.convertToJava(varchar.convertToInternal("Long Text")),
					"AvroVarchar conversion failed with String");
			assertThrows(
					AvroDataTypeException.class,
					() -> { varchar.convertToJava(varchar.convertToInternal("ö")); },
					"AvroVarchar conversion does nto throw exception on non-ASCII chars");

			/*
				| AvroType            | Supported Java types         |      |
				|---------------------|------------------------------|------|
				| AvroDate            | LocalDate, Integer, Number, LocalDateTime, Date, ZonedDateTime, Instant | Extracts the UTC epochDays |
				| AvroTime            | LocalTime, Integer, Number, LocalDateTime, Date, ZonedDateTime, Instant | Extracts the UTC MilliSecondsOfDay |
				| AvroTimeMicros      | LocalTime, Integer, Number, LocalDateTime, Date, ZonedDateTime, Instant | Extracts the UTC MicroSecondsOfDay |
				| AvroTimstamp        | Instant, Long, Date, ZonedDateTime         | Extracts the UTC epoch milliseconds |
				| AvroTimestampMicros | Instant, Long, Date, ZonedDateTime         | Extracts the UTC epoch microseconds |
				| AvroLocalTimstamp        | **LocalDateTime**, Long, Instant, Date, ZonedDateTime         | Extracts the UTC epoch milliseconds |
				| AvroLocalTimestampMicros | **LocalDateTime**, Long, Instant, Date, ZonedDateTime         | Extracts the UTC epoch microseconds |
			 */
			AvroDate date = AvroDate.create();
			assertEquals(nowlocaldate,
					date.convertToJava(date.convertToInternal(nowlocaldate)),
					"AvroDate conversion failed with LocalDate");
			assertEquals(nowlocaldate,
					date.convertToJava(date.convertToInternal((int) nowlocaldate.toEpochDay())),
					"AvroDate conversion failed with Integer");
			assertEquals(nowlocaldate,
					date.convertToJava(date.convertToInternal(LocalDateTime.ofInstant(nowinstant, ZoneId.of("UTC")))),
					"AvroDate conversion failed with LocalDateTime");
			assertEquals(nowlocaldate,
					date.convertToJava(date.convertToInternal(Date.from(nowinstant))),
					"AvroDate conversion failed with Date");
			assertEquals(nowlocaldate,
					date.convertToJava(date.convertToInternal(ZonedDateTime.ofInstant(nowinstant, ZoneId.of("+5")))),
					"AvroDate conversion failed with ZonedDateTime");
			assertEquals(nowlocaldate,
					date.convertToJava(date.convertToInternal(nowinstant)),
					"AvroDate conversion failed with Instant");

			AvroTime time = AvroTime.create();
			LocalTime nowlocaltimewithmillis = nowlocaltime.withNano((nowlocaltime.getNano()/1000000)*1000000);
			assertEquals(nowlocaltimewithmillis,
					time.convertToJava(time.convertToInternal(nowlocaltime)),
					"AvroTime conversion failed with LocalTime");
			assertEquals(nowlocaltimewithmillis,
					time.convertToJava(time.convertToInternal((int) nowlocaltime.getLong(ChronoField.MILLI_OF_DAY))),
					"AvroTime conversion failed with Integer");
			assertEquals(nowlocaltimewithmillis,
					time.convertToJava(time.convertToInternal(LocalDateTime.ofInstant(nowinstant, ZoneId.of("UTC")))),
					"AvroTime conversion failed with LocalDateTime");
			assertEquals(nowlocaltimewithmillis,
					time.convertToJava(time.convertToInternal(Date.from(nowinstant))),
					"AvroTime conversion failed with Date");
			assertEquals(nowlocaltimewithmillis,
					time.convertToJava(time.convertToInternal(ZonedDateTime.ofInstant(nowinstant, ZoneId.of("+5")))),
					"AvroTime conversion failed with ZonedDateTime");
			assertEquals(nowlocaltimewithmillis,
					time.convertToJava(time.convertToInternal(nowinstant)),
					"AvroTime conversion failed with Instant");

			AvroTimeMicros timem = AvroTimeMicros.create();
			LocalTime nowlocaltimewithmicros = nowlocaltime.withNano((nowlocaltime.getNano()/1000)*1000);
			assertEquals(nowlocaltimewithmicros,
					timem.convertToJava(timem.convertToInternal(nowlocaltime)),
					"AvroTimeMicros conversion failed with LocalTime");
			assertEquals(nowlocaltimewithmicros,
					timem.convertToJava(timem.convertToInternal(nowlocaltime.getLong(ChronoField.MICRO_OF_DAY))),
					"AvroTimeMicros conversion failed with Long");
			assertEquals(nowlocaltimewithmicros,
					timem.convertToJava(timem.convertToInternal(LocalDateTime.ofInstant(nowinstant, ZoneId.of("UTC")))),
					"AvroTimeMicros conversion failed with LocalDateTime");
			assertEquals(nowlocaltimewithmillis, // Date does not have micro seconds
					timem.convertToJava(timem.convertToInternal(Date.from(nowinstant))),
					"AvroTimeMicros conversion failed with Date");
			assertEquals(nowlocaltimewithmicros,
					timem.convertToJava(timem.convertToInternal(ZonedDateTime.ofInstant(nowinstant, ZoneId.of("+5")))),
					"AvroTimeMicros conversion failed with ZonedDateTime");
			assertEquals(nowlocaltimewithmicros,
					timem.convertToJava(timem.convertToInternal(nowinstant)),
					"AvroTimeMicros conversion failed with Instant");

			AvroTimestamp ts = AvroTimestamp.create();
			Instant nowts = Instant.ofEpochMilli(nowinstant.toEpochMilli());
			assertEquals(nowts,
					ts.convertToJava(ts.convertToInternal(nowinstant)),
					"AvroTimestamp conversion failed with Instant");
			assertEquals(nowts,
					ts.convertToJava(ts.convertToInternal(nowinstant.toEpochMilli())),
					"AvroTimestamp conversion failed with Long");
			assertEquals(nowts,
					ts.convertToJava(ts.convertToInternal(Date.from(nowinstant))),
					"AvroTimestamp conversion failed with Date");
			assertEquals(nowts,
					ts.convertToJava(ts.convertToInternal(ZonedDateTime.ofInstant(nowinstant, ZoneId.of("+5")))),
					"AvroTimestamp conversion failed with ZonedDateTime");

			AvroTimestampMicros tsmicro = AvroTimestampMicros.create();
			Instant nowtsmicros = Instant.ofEpochSecond(nowinstant.getEpochSecond(), (nowinstant.getNano()/1000)*1000);
			assertEquals(nowtsmicros,
					tsmicro.convertToJava(tsmicro.convertToInternal(nowinstant)),
					"AvroTimestampMicros conversion failed with Instant");
			assertEquals(nowtsmicros,
					tsmicro.convertToJava(tsmicro.convertToInternal(nowinstant.getEpochSecond() * 1000000L + nowinstant.getNano()/1000)),
					"AvroTimestampMicros conversion failed with Long");
			assertEquals(nowts,  // Date does support milliseconds only
					tsmicro.convertToJava(tsmicro.convertToInternal(Date.from(nowinstant))),
					"AvroTimestampMicros conversion failed with Date");
			assertEquals(nowtsmicros,
					tsmicro.convertToJava(tsmicro.convertToInternal(ZonedDateTime.ofInstant(nowinstant, ZoneId.of("+5")))),
					"AvroTimestampMicros conversion failed with ZonedDateTime");

			AvroLocalTimestamp lts = AvroLocalTimestamp.create();
			LocalDateTime nowlts = LocalDateTime.ofEpochSecond(nowinstant.getEpochSecond(), 1000000*(nowinstant.getNano()/1000000), ZoneOffset.UTC);
			assertEquals(nowlts,
					lts.convertToJava(lts.convertToInternal(nowlts)),
					"AvroTimestamp conversion failed with LocalDateTime");
			assertEquals(nowlts,
					lts.convertToJava(lts.convertToInternal(nowinstant)),
					"AvroTimestamp conversion failed with Instant");
			assertEquals(nowlts,
					lts.convertToJava(lts.convertToInternal(nowinstant.toEpochMilli())),
					"AvroTimestamp conversion failed with Long");
			assertEquals(nowlts,
					lts.convertToJava(lts.convertToInternal(Date.from(nowinstant))),
					"AvroTimestamp conversion failed with Date");
			assertEquals(nowlts,
					lts.convertToJava(lts.convertToInternal(ZonedDateTime.ofInstant(nowinstant, ZoneId.of("+5")))),
					"AvroTimestamp conversion failed with ZonedDateTime");

			AvroLocalTimestampMicros ltsmicro = AvroLocalTimestampMicros.create();
			LocalDateTime nowltsmicro = LocalDateTime.ofEpochSecond(nowinstant.getEpochSecond(), 1000*(nowinstant.getNano()/1000), ZoneOffset.UTC);
			LocalDateTime nowltsmicro2 = LocalDateTime.ofEpochSecond(nowinstant.getEpochSecond(), 1000000*(nowinstant.getNano()/1000000), ZoneOffset.UTC);
			assertEquals(nowltsmicro,
					ltsmicro.convertToJava(ltsmicro.convertToInternal(nowinstant)),
					"AvroTimestampMicros conversion failed with Instant");
			assertEquals(nowltsmicro,
					ltsmicro.convertToJava(ltsmicro.convertToInternal(nowinstant.getEpochSecond() * 1000000L + nowinstant.getNano()/1000)),
					"AvroTimestampMicros conversion failed with Long");
			assertEquals(nowltsmicro2,  // Date does support milliseconds only
					ltsmicro.convertToJava(ltsmicro.convertToInternal(Date.from(nowinstant))),
					"AvroTimestampMicros conversion failed with Date");
			assertEquals(nowltsmicro,
					ltsmicro.convertToJava(ltsmicro.convertToInternal(ZonedDateTime.ofInstant(nowinstant, ZoneId.of("+5")))),
					"AvroTimestampMicros conversion failed with ZonedDateTime");

			/*
				| AvroType    | Supported Java types         |      |
				|-------------|------------------------------|------|
				| AvroStGeometry | CharSequence, Object      |      |
				| AvroSTPoint | CharSequence, Object         |      |
			 */
			AvroSTGeometry geo = AvroSTGeometry.create();
			assertEquals("Hello STGeometry", geo.convertToJava(geo.convertToInternal("Hello STGeometry")), "AvroSTGeometry conversion failed with String");
			AvroSTPoint pt = AvroSTPoint.create();
			assertEquals("Hello STPoint", pt.convertToJava(pt.convertToInternal("Hello STPoint")), "AvroSTPoint conversion failed with String");

			/*
				| AvroType    | Supported Java types          |      |
				|-------------|-------------------------------|------|
				| AvroEnum    | Enum<?>, CharSequence, Object |      |
				| AvroMap     | Map<String, ?>                |      |
				| AvroArray   | List<?>, Object[]             |      |
			 */
			AvroEnum e = AvroEnum.create(RowType.class);
			assertEquals(RowType.INSERT.name(), e.convertToJava(e.convertToInternal(RowType.INSERT)), "AvroEnum conversion failed with Enum");
			assertEquals(RowType.INSERT.name(), e.convertToJava(e.convertToInternal(RowType.INSERT.name())), "AvroEnum conversion failed with String");

			AvroMap m = AvroMap.create(AvroInt.create());
			Map<String, Integer> map = Collections.singletonMap("KEY", 22);
			assertEquals(map, m.convertToJava(m.convertToInternal(map)), "AvroMap conversion failed with Enum");

			AvroArray array = AvroArray.create(AvroInt.create());
			List<Integer> arraydata = Collections.singletonList(22);
			assertEquals(arraydata, array.convertToJava(array.convertToInternal(arraydata)), "AvroArray conversion failed with List");

			/*
				| AvroType    | Supported Java types         |      |
				|-------------|------------------------------|------|
				| AvroBoolean | **Boolean**, String, Number  | for text: TRUE/FALSE ignoring case; for numbers: 1/0 |
				| AvroBytes   | **ByteBuffer**, byte[]       |      |
				| AvroFixed   | **ByteBuffer**, byte[]       |      |
				| AvroUri     | **CharSequence**, Object     |      |
				| AvroUUID    | **CharSequence**, Object     |      |
				| AvroAnyPrimitive | **Object**              | not validated |
			 */
			AvroBoolean b = AvroBoolean.create();
			assertEquals(Boolean.TRUE, b.convertToJava(b.convertToInternal(true)), "AvroBoolean conversion failed with boolean");
			assertEquals(Boolean.TRUE, b.convertToJava(b.convertToInternal("True")), "AvroBoolean conversion failed with String");
			assertEquals(Boolean.TRUE, b.convertToJava(b.convertToInternal(1)), "AvroBoolean conversion failed with Number");

			AvroBytes bytes = AvroBytes.create();
			ByteBuffer bytesdata = ByteBuffer.wrap("Hello bytes".getBytes());
			assertEquals(bytesdata.array(), bytes.convertToJava(bytes.convertToInternal(bytesdata)), "AvroBytes conversion failed with ByteBuffer");
			assertEquals(bytesdata.array(), bytes.convertToJava(bytes.convertToInternal(bytesdata.array())), "AvroBytes conversion failed with byte[]");

			AvroFixed fixed = AvroFixed.create(20);
			assertEquals(bytesdata.array(), fixed.convertToJava(fixed.convertToInternal(bytesdata)), "AvroFixed conversion failed with ByteBuffer");
			assertEquals(bytesdata.array(), fixed.convertToJava(fixed.convertToInternal(bytesdata.array())), "AvroFixed conversion failed with byte[]");

			AvroUri uri = AvroUri.create();
			assertEquals("Hello Uri", uri.convertToJava(uri.convertToInternal("Hello Uri")), "AvroUri conversion failed with String");

			AvroUUID uuid = AvroUUID.create();
			assertEquals("Hello UUID", uuid.convertToJava(uuid.convertToInternal("Hello UUID")), "AvroUUID conversion failed with String");

			AvroAnyPrimitive any = AvroAnyPrimitive.create();
			assertEquals(77, any.convertToJava(any.convertToInternal(77)), "AvroPrimitive conversion failed with Integer");

		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}


}
