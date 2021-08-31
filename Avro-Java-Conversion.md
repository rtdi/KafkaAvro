# Conversion from/to Java/Avro


## Setting Avro values

Using the helper method `AvroType.putRecordValue(GenericRecord record, String fieldname, Object value)` the values of fields can be set including the conversion into the Avro expected data type.

The native way doing that in Avro is either by setting the expected value manually or adding custom Conversion classes. Either way, any error is returned at serialization time which is often too late.

### Numbers

| AvroType    | Supported Java types             | Note |
|-------------|----------------------------------|------|
| AvroInt     | **Integer**, String, Number      |      |
| AvroByte    | **Integer**, String, Number      | value must be in Java Byte range |
| AvroDecimal | **BigDecimal**, String, Number   |      |
| AvroDouble  | **Double**, String, Number       |      |
| AvroFloat   | **Float**, String, Number        |      |
| AvroLong    | **Long**, String, Number         |      |
| AvroShort   | **Integer**, String, Number      | value must be in Java Short range |

### Text

| AvroType     | Supported Java types             |      |
|--------------|----------------------------------|------|
| AvroCLOB     | **CharSequence**, Object         | error in case non-ASCII chars are found     |
| AvroNCLOB    | **CharSequence**, Object         |      |
| AvroNVarchar | **CharSequence**, Object         | longer texts are truncated     |
| AvroString   | **CharSequence**, Object         |      |
| AvroVarchar  | **CharSequence**, Object         | longer texts are truncated, error in case non-ASCII chars are found |

### Dates

| AvroType            | Supported Java types         |      |
|---------------------|------------------------------|------|
| AvroDate            | **LocalDate**, Integer, Number, LocalDateTime, Date, ZonedDateTime, Instant | Extracts the UTC epochDays |
| AvroTime            | **LocalTime**, Integer, Number, LocalDateTime, Date, ZonedDateTime, Instant | Extracts the UTC MilliSecondsOfDay |
| AvroTimeMicros      | **LocalTime**, Integer, Number, LocalDateTime, Date, ZonedDateTime, Instant | Extracts the UTC MicroSecondsOfDay |
| AvroTimstamp        | **Instant**, Long, Date, ZonedDateTime         | Extracts the UTC epoch milliseconds |
| AvroTimestampMicros | **Instant**, Long, Date, ZonedDateTime         | Extracts the UTC epoch microseconds |


### Spatial

| AvroType       | Supported Java types         |      |
|----------------|------------------------------|------|
| AvroStGeometry | **CharSequence**, Object     |      |
| AvroSTPoint    | **CharSequence**, Object     |      |

### Complex

| AvroType    | Supported Java types              |      |
|-------------|-----------------------------------|------|
| AvroEnum    | **CharSequence**, Enum<?>, Object |      |
| AvroMap     | **Map<String, ?>**                |      |
| AvroArray   | **List<?>**, Object[]             |      |

### Other

| AvroType         | Supported Java types             |      |
|------------------|----------------------------------|------|
| AvroBoolean      | **Boolean**, String, Number      | for text: TRUE/FALSE ignoring case; for numbers: 1/0 |
| AvroBytes        | **byte[]**, ByteBuffer           |      |
| AvroFixed        | **byte[]**, ByteBuffer           |      |
| AvroUri          | **CharSequence**, Object         |      |
| AvroUUID         | **CharSequence**, Object         |      |
| AvroAnyPrimitive | **Object**                       | not validated |

