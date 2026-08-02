# Kafka Avro convenience methods

When using Avro messages in the context of Kafka as Data Integration solution, there are a couple of common problems.
All of them are easy to solve and this library aims to help.

## Common problems in the Kafka-Avro world

The types of problems can be separated into

 - Coding quirks: How to create a schema matching a database table, converting the database data type into the Avro datatype, column names using chars not allowed in Avro,...
 - Missing metadata: A varchar(1) is in Avro a String, hence when loading the message into a target database, the length information is no longer known. The library provides areas to store way more metadata.
 - Missing message formats: For a proper data integration solution we need to store commits, impact/lineage information, dependencies.

### No detailed data type information

**Problem**: Avro supports base data types like String, Integer and the such plus a few logical data types like TimeMillis. But if data is moved from one database to another database via Kafka, what should the target database use as a data type?

**Solution**: Provide more Logical Data Types for Avro. Avro allows adding custom data types and they just annotate the schema. Zero impact to any existing code, because logical types provide more information but are based on standard Avro datatypes, aka backing types.

**Example**

Create an Avro field name `CUSTOMER_ID`, its Avro type is `string` but add the SQL datatype `NVARCHAR(10)` information also.

```
ValueSchema valueschema = new ValueSchema("Schema1", null);
valueschema.add("CUSTOMER_ID", AvroNVarchar.create(10), null, false);
```

This is turned into an Avro Logical type, hence the resulting field definition looks like

```
{
    "name" : "CUSTOMER_ID",
    "type" : {
        "type" : "string",
        "logicalType" : "NVARCHAR",
        "length" : 10
    },
    "__originalname" : "CUSTOMER_ID"
}
```

The library provides all SQL standard data types as Avro logical types plus the Avro standard types as well.

In addition, a field can also store a free form text with the concrete data type.

```
valueschema.add("CUSTOMER_ID", AvroNVarchar.create(10), null, false).setSourceDataType("SAP NUMC(10)");
```


### Conversion Java<-->Avro

**Problem**: Avro lets the user write any value into the Record, it will fail at the serialization time if not supported. Also the Avro internal conversion system is so flexible that it is difficult to use and very limited in the from/to conversion options. It would be nice if the conversion logic is a bit more graceful.

**Solution**: Every data type class provides conversion functions that are invoked automatically.

**Example**

The column `ORDER_DATE` is a date datatype, a logical type AvroDate. When creating a record for this schema and setting the value, what is the correct Java object?
Could be many things:

 - Integer = 20201231
 - LocalDateTime
 - LocalDate
 - Date
 - ZonedDateTime
 - Instant
 - String = "20201231"

The `AvroDate` class provides conversion functions for all directions.

```
GenericRecord data = new GenericData.Record(schema);
AvroType.putRecordValue(data, "ORDER_DATE", nowinstant);
```

### Avro native data types and Logical types are used differently

**Problem**: When working with the Avro record, the user must check if the field is a logical data type and invoke the required code to read it as logical data type. That is a lot of if-then-else logic on every single field.

**Solution**: All data types are annotated as logical data types, even the base data types. This has no down sides, no performance penalty and makes life more convenient. When reading from an Avro schema that has no annotations, the logical data type is derived from the base data type automatically.

**Example**

Even an Avro string exists as logical data type `AvroString` and the `AvroType` class has methods to derive the proper data type from an existing schema. If the field is annotated as a logical type, this is used, otherwise the Avro data type is used to return the matching Avro logical type of this library.


### Data type groups

**Problem**: Often different methods must be called depending on the data type group.

**Solution**: Providing more metadata about the data types like length based? Textual? Date?

**Example**

A setString() method for every data type that is text related but a setNumber() for all number data types. The library has one generic method for all data types.

```
AvroType.putRecordValue(data, "ORDER_DATE", nowinstant);
```


### Schema and Field names are very restricted in Avro

**Problem**: A SAP database has a column called "/BIC/MANDT000". This is not a valid Avro field name. What to do now?

**Solution**: Name converters to turn names into Avro supported names and vice versa. The change should be as little as possible.

**Example**:

Creating a schema, adding a field, specifying a name in a named Avro data type (e.g. enum), all invokes the AvroNameEncoder.encodeName() method.

```
valueschema.add("/BIC/MANDT000", AvroNVarchar.create(10), null, false);
```


### Add more metadata to the schema

**Problem**: Initially in the project only the data is put into the messages. Once the solution is in production users will ask questions like

 - What are the primary keys of this record?
 - All foreign key infomration
 - Security info
 - Data Product related info

**Solution**: Add optional properties to the schema to provide space for this kind of information.

**Example**

The `ValueSchema` contains additional fields to store this kind of information.

```
ValueSchema valueschema = new ValueSchema("Schema1", null);
valueschema.setPrimaryKeys();
valueschema.setForeignKeys();
valueschema.setRegulations();
valueschema.setRetentionPeriod();
valueschema.setDeletionPolicy();
valueschema.setDataProductOwner();
valueschema.setTicketUrl();
valueschema.setRepoUrl();
valueschema.setObjectLevelSecurity();
valueschema.setRowLevelSecurity();
valueschema.setPartitionBy();
valueschema.setSemantics();
```


### Add standard data integration fields to the `ValueSchema`

**Problem**: Initially in the project only the data is put into the messages. Once the solution is in production users will ask questions like

 - When was the commit time of a the change in the source?
 - What was the transaction id of the change?
 - What was the source system of this particular row (e.g. if there are two source systems with changes for the same target)?
 - What data quaility checks have been applied, what was their individual result and the voerall result? (AUDIT structure)

**Solution**: Add optional fields to the schema to provide space for this kind of information.

**Example**

The `ValueSchema` contains additional fields to store this kind of information.

```
ValueSchema valueschema = new ValueSchema("Schema1", null);
GenericRecord data = new GenericData.Record(valueschema);
AvroType.putRecordValue(data, SchemaConstants.SCHEMA_COLUMN_CHANGE_TIME, value);
AvroType.putRecordValue(data, SchemaConstants.SCHEMA_COLUMN_SOURCE_ROWID, value);
AvroType.putRecordValue(data, SchemaConstants.SCHEMA_COLUMN_SOURCE_TRANSACTION, value);
AvroType.putRecordValue(data, SchemaConstants.SCHEMA_COLUMN_SOURCE_SYSTEM, value);
AvroType.putRecordValue(data, SchemaConstants.AUDIT, value);
```


### How is the record to be treated? Insert, delete,..?

**Problem**: In a Data Integration scenario data is not only appended but also deleted, updated, etc. How does the consumer know? The producer must provide that information.

**Solution**: Have a naming convention on how a produce provides this information in the record.

**Example**

In the source database a delete of customer=1234 happened. The expectation is that this record flows through Kafka and a database consumer does delete the record from the target database.

```
ValueSchema valueschema = new ValueSchema("Schema1", null);
GenericRecord data = new GenericData.Record(valueschema);
AvroType.putRecordValue(data, SchemaConstants.SCHEMA_COLUMN_CHANGE_TYPE, RowType.DELETE.getIdentifer());
```

Note: For truncates there is an additional field value `SchemaConstants.SCHEMA_COLUMN_TRUNCATE` that defines the filter of the truncate. If empty, the entire table is to be truncated, it is an initial load.


### A Schema Builder better suited for dynamic creation

**Problem**: The Avro SchemaBuilder is well suited for fixed, well known schemas.

**Solution**: An alternative SchemaBuilder better suited for such recursive calls.

**Example**

The Avro library does focus on fluent builders where the schema is built manually. For data integration use cases the requirements are different. A source database's data dictionary is read and the information
retrieved does modify the schema one step at a time.

Also a schema can be described in different flavors, depending on the use case:

 - A Java representation, used when building the schema manually using Java constructors and methods.
 - A Json representation useful for UIs, e.g. in Avro a nullable column has a type `[null, string]`, but in an UI the table of columns should show: name, datatype, nullable,...
 - A Java representation of the actual Avro Schema, e.g. needed when creating a new record `new GenericData.Record(valueschema);`
 - A Json representation of the Avro SChema, e.g. when saving the schema in the schema registry.

Converting back and forth between these formats is supported and lossless.

Hence the library allows to:

 - Read the schema registry, convert it to an e.g. ValueSchema class and show it in an UI.
 - Use the convienence functions to create an e.g. ValueSchema manually and save it in the Schema Registry.
 - Derive the Avro Schema out of an e.g. ValueSchema.



### Schemas must support an extension concept

**Problem**: Often multiple producers create data for the same schema. Hence the schema must be the superset of all. While that does make sense for most fields, some are more of technical nature and do not deserve an individual field just for itself.

**Solution**: Each schema level has an __extension array to store key-value pairs.

**Example**

The source system has a gender column of type string, the Avro schema a gender as integer. The producer does convert each string to the official value but it would be nice if the original value is stored in the record as well somewhere for auditing purposes.

```
Map<String, String> kv = new HashMap<>();
kv.put("GENDER_IN_SOURCE", 1);
AvroType.putRecordValue(data, SchemaConstants.SCHEMA_COLUMN_EXTENSION_MAP, kv);
```

