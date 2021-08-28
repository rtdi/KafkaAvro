# Kafka Avro convenience methods

When using Avro messages in the context of Kafka as Data Integration solution, there are a couple of common problems.
All of them are easy to solve and this library aims to help.

## Common problems in the Kafka-Avro world

### No detailed data type information

**Problem**: Avro supports base data types like String, Integer and the such plus a few logical data types like TimeMillis. But if data is moved from one database to another database via Kafka, what should the target database use as a data type?

**Example**: Source database has a NVARCHAR(10). In the Avro Schema that must be a String. If the table in the target is created based on the Avro Schema, the only proper data type for this column is NCLOB. With all the performance penalty involved with LOB datatypes.

**Solution**: Provide more Logical Data Types for Avro. Avro allows adding custom data types and they just annotate the schema. Zero impact to any existing code.

### Conversion Java<-->Avro

**Problem**: Avro lets the user write any value into the Record, it will fail at the serialization time if not supported. Also the Avro internal conversion system is so flexible that it is difficult to use and very limited in the from/to conversion options. It would be nice if the conversion logic is a bit more graceful.

**Example**: An Avro Boolean requires a Java Boolean. But how often does the source rather provide an integer with the values 1/0 for true/false. Or a string TRUE/FALSE?

**Solution**: Every data type class provides conversion functions.

### Avro native data types and Logical types are used differently

**Problem**: When working with the Avro record, the user must check if the field is a logical data type and invoke the required code to read it as logical data type. That is a lot of if-then-else logic on every single field.

**Solution**: All data types are annotated as logical data types, even the base data types. This has no down sides, no performance penalty and make life more convenient. When reading from an Avro schema that has no annotations, the logical data type is derived from the base data type automatically.

### Data type groups

**Problem**: Often different methods must be called depending on the data type group.

**Example**: A setString() method for every data type that is text related but a setNumber() for all number data types.

**Solution**: Providing more metadata about the data types like length based? Textual? Date?


### Schema and Field names are very restricted in Avro

**Problem**: A database has a column called "/BIC/MANDT000". This is no valid Avro field name.

**Solution**: Name converters to turn not-supported names into Avro supported names and vice versa. The change should be as little as possible.


### Add more metadata to the schema

**Problem**: Initially in the project only the data is put into the messages. Once the solution is in production users will ask questions like

* Where did the record originate from? This topic contains records from multiple source systems.
* Why was the record sent by the source source system? Who triggered it, what is the location within the source (e.g. rowid)?
* Which transformations did the record undergo and what was the success of the transformation?

**Solution**: Add optional fields to the schema to provide space for this kind of information.