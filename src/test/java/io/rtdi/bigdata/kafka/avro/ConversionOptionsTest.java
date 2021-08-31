package io.rtdi.bigdata.kafka.avro;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.rtdi.bigdata.kafka.avro.datatypes.*;

public class ConversionOptionsTest {
	private static Instant nowinstant = Instant.now();
	private static LocalDate nowlocaldate = LocalDate.ofInstant(nowinstant, ZoneId.of("UTC")); 
	private static LocalTime nowlocaltime = LocalTime.ofInstant(nowinstant, ZoneId.of("UTC"));
	
	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

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
			assertEquals("AvroInt conversion failed with Integer", Integer.valueOf(23),
					i.convertToJava(i.convertToInternal(23)));
			assertEquals("AvroInt conversion failed with String", Integer.valueOf(23),
					i.convertToJava(i.convertToInternal("23")));
			assertEquals("AvroInt conversion failed with Float", Integer.valueOf(23),
					i.convertToJava(i.convertToInternal(23.7f)));
			
			AvroDecimal d = AvroDecimal.create(20, 7);
			assertEquals("AvroDecimal conversion failed with BigDecimal", BigDecimal.valueOf(239000000, 7), 
					d.convertToJava(d.convertToInternal(BigDecimal.valueOf(23.9d))));
			assertEquals("AvroDecimal conversion failed with String", BigDecimal.valueOf(239000000, 7), 
					d.convertToJava(d.convertToInternal("23.9")));
			assertEquals("AvroDecimal conversion failed with Float", BigDecimal.valueOf(239000000, 7),
					d.convertToJava(d.convertToInternal(23.9f)));

			AvroDouble dd = AvroDouble.create();
			assertEquals("AvroDouble conversion failed with Double", Double.valueOf(23.9),
					dd.convertToJava(dd.convertToInternal(23.9)));
			assertEquals("AvroDouble conversion failed with String", Double.valueOf(23.9),
					dd.convertToJava(dd.convertToInternal("23.9")));
			assertEquals("AvroDouble conversion failed with Float", Double.valueOf(23.9),
					dd.convertToJava(dd.convertToInternal(23.9f)));

			AvroFloat f = AvroFloat.create();
			assertEquals("AvroFloat conversion failed with Float", Float.valueOf(23.9f),
					f.convertToJava(f.convertToInternal(23.9f)));
			assertEquals("AvroFloat conversion failed with String", Float.valueOf(23.9f),
					f.convertToJava(f.convertToInternal("23.9")));
			assertEquals("AvroFloat conversion failed with Double", Float.valueOf(23.9f),
					f.convertToJava(f.convertToInternal(23.9)));

			AvroLong l = AvroLong.create();
			assertEquals("AvroLong conversion failed with Long", Long.valueOf(-45L),
					l.convertToJava(l.convertToInternal(-45L)));
			assertEquals("AvroLong conversion failed with String", Long.valueOf(-45L),
					l.convertToJava(l.convertToInternal("-45")));
			assertEquals("AvroLong conversion failed with Double", Long.valueOf(-45L),
					l.convertToJava(l.convertToInternal(-45.0)));

			AvroShort s = AvroShort.create();
			assertEquals("AvroShort conversion failed with Short", Short.valueOf((short) 23),
					s.convertToJava(s.convertToInternal((short) 23)));
			assertEquals("AvroShort conversion failed with String", Short.valueOf((short) 23),
					s.convertToJava(s.convertToInternal("23")));
			assertEquals("AvroShort conversion failed with Double", Short.valueOf((short) 23),
					s.convertToJava(s.convertToInternal(23.0)));

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
			assertEquals("AvroCLOB conversion failed with String", "Long Text", clob.convertToJava(clob.convertToInternal("Long Text")));
			assertThrows("AvroCLOB conversion does nto throw exception on non-ASCII chars", 
					AvroDataTypeException.class,
					() -> { clob.convertToJava(clob.convertToInternal("ö")); });

			AvroNCLOB nclob = AvroNCLOB.create();
			assertEquals("AvroNCLOB conversion failed with String", "Long Text", 
					nclob.convertToJava(nclob.convertToInternal("Long Text")));

			AvroNVarchar nvarchar = AvroNVarchar.create(3);
			assertEquals("AvroNVarchar conversion failed with String", "Lon", 
					nvarchar.convertToJava(nvarchar.convertToInternal("Long Text")));
			
			AvroString astring = AvroString.create();
			assertEquals("AvroString conversion failed with String", "Long Text", 
					astring.convertToJava(astring.convertToInternal("Long Text")));

			AvroVarchar varchar = AvroVarchar.create(3);
			assertEquals("AvroVarchar conversion failed with String", "Lon", 
					varchar.convertToJava(varchar.convertToInternal("Long Text")));
			assertThrows("AvroVarchar conversion does nto throw exception on non-ASCII chars", 
					AvroDataTypeException.class,
					() -> { varchar.convertToJava(varchar.convertToInternal("ö")); });

			/*
				| AvroType            | Supported Java types         |      |
				|---------------------|------------------------------|------|
				| AvroDate            | LocalDate, Integer, Number, LocalDateTime, Date, ZonedDateTime, Instant | Extracts the UTC epochDays |
				| AvroTime            | LocalTime, Integer, Number, LocalDateTime, Date, ZonedDateTime, Instant | Extracts the UTC MilliSecondsOfDay |
				| AvroTimeMicros      | LocalTime, Integer, Number, LocalDateTime, Date, ZonedDateTime, Instant | Extracts the UTC MicroSecondsOfDay |
				| AvroTimstamp        | Instant, Long, Date, ZonedDateTime         | Extracts the UTC epoch milliseconds |
				| AvroTimestampMicros | Instant, Long, Date, ZonedDateTime         | Extracts the UTC epoch microseconds |
			 */
			AvroDate date = AvroDate.create();
			assertEquals("AvroDate conversion failed with LocalDate", nowlocaldate, 
					date.convertToJava(date.convertToInternal(nowlocaldate)));
			assertEquals("AvroDate conversion failed with Integer", nowlocaldate, 
					date.convertToJava(date.convertToInternal((int) nowlocaldate.toEpochDay())));
			assertEquals("AvroDate conversion failed with LocalDateTime", nowlocaldate, 
					date.convertToJava(date.convertToInternal(LocalDateTime.ofInstant(nowinstant, ZoneId.of("UTC")))));
			assertEquals("AvroDate conversion failed with Date", nowlocaldate, 
					date.convertToJava(date.convertToInternal(Date.from(nowinstant))));
			assertEquals("AvroDate conversion failed with ZonedDateTime", nowlocaldate, 
					date.convertToJava(date.convertToInternal(ZonedDateTime.ofInstant(nowinstant, ZoneId.of("+5")))));
			assertEquals("AvroDate conversion failed with Instant", nowlocaldate,
					date.convertToJava(date.convertToInternal(nowinstant)));

			AvroTime time = AvroTime.create();
			LocalTime nowlocaltimewithmillis = nowlocaltime.withNano(((int) nowlocaltime.getNano()/1000000)*1000000);
			assertEquals("AvroTime conversion failed with LocalTime", nowlocaltimewithmillis, 
					time.convertToJava(time.convertToInternal(nowlocaltime)));
			assertEquals("AvroTime conversion failed with Integer", nowlocaltimewithmillis, 
					time.convertToJava(time.convertToInternal((int) nowlocaltime.getLong(ChronoField.MILLI_OF_DAY))));
			assertEquals("AvroTime conversion failed with LocalDateTime", nowlocaltimewithmillis, 
					time.convertToJava(time.convertToInternal(LocalDateTime.ofInstant(nowinstant, ZoneId.of("UTC")))));
			assertEquals("AvroTime conversion failed with Date", nowlocaltimewithmillis, 
					time.convertToJava(time.convertToInternal(Date.from(nowinstant))));
			assertEquals("AvroTime conversion failed with ZonedDateTime", nowlocaltimewithmillis, 
					time.convertToJava(time.convertToInternal(ZonedDateTime.ofInstant(nowinstant, ZoneId.of("+5")))));
			assertEquals("AvroTime conversion failed with Instant", nowlocaltimewithmillis,
					time.convertToJava(time.convertToInternal(nowinstant)));

			AvroTimeMicros timem = AvroTimeMicros.create();
			LocalTime nowlocaltimewithmicros = nowlocaltime.withNano(((int) nowlocaltime.getNano()/1000)*1000);
			assertEquals("AvroTimeMicros conversion failed with LocalTime", nowlocaltimewithmicros, 
					timem.convertToJava(timem.convertToInternal(nowlocaltime)));
			assertEquals("AvroTimeMicros conversion failed with Long", nowlocaltimewithmicros, 
					timem.convertToJava(timem.convertToInternal(nowlocaltime.getLong(ChronoField.MICRO_OF_DAY))));
			assertEquals("AvroTimeMicros conversion failed with LocalDateTime", nowlocaltimewithmicros, 
					timem.convertToJava(timem.convertToInternal(LocalDateTime.ofInstant(nowinstant, ZoneId.of("UTC")))));
			assertEquals("AvroTimeMicros conversion failed with Date", nowlocaltimewithmillis, // Date does not have micro seconds
					timem.convertToJava(timem.convertToInternal(Date.from(nowinstant))));
			assertEquals("AvroTimeMicros conversion failed with ZonedDateTime", nowlocaltimewithmicros, 
					timem.convertToJava(timem.convertToInternal(ZonedDateTime.ofInstant(nowinstant, ZoneId.of("+5")))));
			assertEquals("AvroTimeMicros conversion failed with Instant", nowlocaltimewithmicros,
					timem.convertToJava(timem.convertToInternal(nowinstant)));
			
			AvroTimestamp ts = AvroTimestamp.create();
			Instant nowts = Instant.ofEpochMilli(nowinstant.toEpochMilli());
			assertEquals("AvroTimestamp conversion failed with Instant", nowts,
					ts.convertToJava(ts.convertToInternal(nowinstant)));
			assertEquals("AvroTimestamp conversion failed with Long", nowts, 
					ts.convertToJava(ts.convertToInternal(nowinstant.toEpochMilli())));
			assertEquals("AvroTimestamp conversion failed with Date", nowts, 
					ts.convertToJava(ts.convertToInternal(Date.from(nowinstant))));
			assertEquals("AvroTimestamp conversion failed with ZonedDateTime", nowts, 
					ts.convertToJava(ts.convertToInternal(ZonedDateTime.ofInstant(nowinstant, ZoneId.of("+5")))));

			AvroTimestampMicros tsmicro = AvroTimestampMicros.create();
			Instant nowtsmicros = Instant.ofEpochSecond(nowinstant.getEpochSecond(), ((int) (nowinstant.getNano()/1000))*1000);
			assertEquals("AvroTimestampMicros conversion failed with Instant", nowtsmicros,
					tsmicro.convertToJava(tsmicro.convertToInternal(nowinstant)));
			assertEquals("AvroTimestampMicros conversion failed with Long", nowtsmicros, 
					tsmicro.convertToJava(tsmicro.convertToInternal(nowinstant.getEpochSecond() * 1000000L + nowinstant.getNano()/1000)));
			assertEquals("AvroTimestampMicros conversion failed with Date", nowts,  // Date does support milliseconds only
					tsmicro.convertToJava(tsmicro.convertToInternal(Date.from(nowinstant))));
			assertEquals("AvroTimestampMicros conversion failed with ZonedDateTime", nowtsmicros, 
					tsmicro.convertToJava(tsmicro.convertToInternal(ZonedDateTime.ofInstant(nowinstant, ZoneId.of("+5")))));
			
			/*
				| AvroType    | Supported Java types         |      |
				|-------------|------------------------------|------|
				| AvroStGeometry | CharSequence, Object      |      |
				| AvroSTPoint | CharSequence, Object         |      |
			 */
			AvroSTGeometry geo = AvroSTGeometry.create();
			assertEquals("AvroSTGeometry conversion failed with String", "Hello STGeometry", geo.convertToJava(geo.convertToInternal("Hello STGeometry")));
			AvroSTPoint pt = AvroSTPoint.create();
			assertEquals("AvroSTPoint conversion failed with String", "Hello STPoint", pt.convertToJava(pt.convertToInternal("Hello STPoint")));

			/*
				| AvroType    | Supported Java types          |      |
				|-------------|-------------------------------|------|
				| AvroEnum    | Enum<?>, CharSequence, Object |      |
				| AvroMap     | Map<String, ?>                |      |
				| AvroArray   | List<?>, Object[]             |      |
			 */
			AvroEnum e = AvroEnum.create(RowType.class);
			assertEquals("AvroEnum conversion failed with Enum", RowType.INSERT.name(), e.convertToJava(e.convertToInternal(RowType.INSERT)));
			assertEquals("AvroEnum conversion failed with String", RowType.INSERT.name(), e.convertToJava(e.convertToInternal(RowType.INSERT.name())));

			AvroMap m = AvroMap.create(AvroInt.create());
			Map<String, Integer> map = Collections.singletonMap("KEY", 22);
			assertEquals("AvroMap conversion failed with Enum", map, m.convertToJava(m.convertToInternal(map)));
			
			AvroArray array = AvroArray.create(AvroInt.create());
			List<Integer> arraydata = Collections.singletonList(22);
			assertEquals("AvroArray conversion failed with List", arraydata, array.convertToJava(array.convertToInternal(arraydata)));
			
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
			assertEquals("AvroBoolean conversion failed with boolean", Boolean.TRUE, b.convertToJava(b.convertToInternal(true)));
			assertEquals("AvroBoolean conversion failed with String", Boolean.TRUE, b.convertToJava(b.convertToInternal("True")));
			assertEquals("AvroBoolean conversion failed with Number", Boolean.TRUE, b.convertToJava(b.convertToInternal(1)));

			AvroBytes bytes = AvroBytes.create();
			ByteBuffer bytesdata = ByteBuffer.wrap("Hello bytes".getBytes());
			assertEquals("AvroBytes conversion failed with ByteBuffer", bytesdata.array(), bytes.convertToJava(bytes.convertToInternal(bytesdata)));
			assertEquals("AvroBytes conversion failed with byte[]", bytesdata.array(), bytes.convertToJava(bytes.convertToInternal(bytesdata.array())));

			AvroFixed fixed = AvroFixed.create(20);
			assertEquals("AvroFixed conversion failed with ByteBuffer", bytesdata.array(), fixed.convertToJava(fixed.convertToInternal(bytesdata)));
			assertEquals("AvroFixed conversion failed with byte[]", bytesdata.array(), fixed.convertToJava(fixed.convertToInternal(bytesdata.array())));

			AvroUri uri = AvroUri.create();
			assertEquals("AvroUri conversion failed with String", "Hello Uri", uri.convertToJava(uri.convertToInternal("Hello Uri")));

			AvroUUID uuid = AvroUUID.create();
			assertEquals("AvroUUID conversion failed with String", "Hello UUID", uuid.convertToJava(uuid.convertToInternal("Hello UUID")));

			AvroAnyPrimitive any = AvroAnyPrimitive.create();
			assertEquals("AvroUUID conversion failed with String", 77, any.convertToJava(any.convertToInternal(77)));

		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}


}
