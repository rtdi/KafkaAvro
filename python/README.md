# Kafka Avro convenience methods

When using Avro messages in the context of Kafka as Data Integration solution, there are a couple of common problems.
All of them are easy to solve and this library aims to help.

## Common problems in the Kafka-Avro world

### No detailed data type information

**Problem**: Avro supports base data types like String, Integer and the such plus a few logical data types like TimeMillis. But if data is moved from one database to another database via Kafka, what should the target database use as a data type?

**Example**: Source database has a NVARCHAR(10). In the Avro Schema that must be a String. If the table in the target is created based on the Avro Schema, the only proper data type for this column is NCLOB. With all the performance penalty involved with LOB datatypes.

**Solution**: Provide more Logical Data Types for Avro. Avro allows adding custom data types and they just annotate the schema. Zero impact to any existing code.


### Schema and Field names are very restricted in Avro

**Problem**: A database has a column called "/BIC/MANDT000". This is not a valid Avro field name.

**Solution**: Name converters to turn names into Avro supported names and vice versa. The change should be as little as possible.


### Add more metadata to the schema

**Problem**: Initially in the project only the data is put into the messages. Once the solution is in production users will ask questions like

* What are the primary keys of this record?
* Where did the record originate from? This topic contains records from multiple source systems.
* Why was the record sent by the source source system? Who triggered it, what is the location within the source (e.g. rowid)?
* Which transformations did the record undergo and what was the success of the transformation?

**Solution**: Add optional fields to the schema to provide space for this kind of information.


### How is the record to be treated? Insert, delete,..?

**Problem**: In a Data Integration scenario data is not only appended but also deleted, updated, etc. How does the consumer know? The producer must provide that information.

**Example**: In the source database a delete of customer=1234 happened. The expectation is that this record flows through Kafka and a database consumer does delete the record from the target database.

**Solution**: Have a naming convention on how a produce provides this information in the record.


### A Schema Builder better suited for dynamic creation

**Problem**: The Avro SchemaBuilder is well suited for fixed, well known schemas.

**Example**: A schema should be built based on a database table metadata, including primary key information, nullable etc.

**Solution**: An alternative SchemaBuilder better suited for such recursive calls.


### Schemas must support an extension concept

**Problem**: Often multiple producers create data for the same schema. Hence the schema must be the superset of all. While that does make sense for most fields, some are more of technical nature and do not deserve an individual field just for itself.

**Example**: The source system has a gender column of type string, the Avro schema a gender as integer. The producer does convert each string to the official value but it would be nice if the original value is stored in the record as well somewhere for auditing purposes.

**Solution**: Each schema level has an __extension array to store key-value pairs.

