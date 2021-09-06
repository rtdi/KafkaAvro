# Avro Data Structures for Data Integration with Kafka

Imagine a table in a source database

|Column name  | Data type     | not null | Key |
|-------------|---------------|----------|-----|
|CUSTOMER_ID  | INT           | true     | 1   |
|COMPANY_NAME | NVARCHAR(30)  |          |     |
|ADDRESS_ID   | INT           | true     |     |
|EMPLOYEES    | INT           |          |     |
|REVENUE$     | DECIMAL(12,0) |          |     |


How should the corresponding Avro schema look like?

A first response would be, exactly the same, so

```
{
  "type":"record","name":"CUSTOMER","fields":
    [
      { "name":"CUSTOMER_ID",  "type":"int"                      },
      { "name":"COMPANY_NAME", "type":["null",{"type":"string"}] },
      { "name":"ADDRESS_ID",   "type":["null",{"type":"int"}]    },
      { "name":"EMPLOYEES",    "type":["null",{"type":"int"}]    },
      { "name":"REVENUE$",     "type":["null",{"type":"bytes", "logicalType":"decimal", "precision":12, "scale":0}] }
    ]
}
```

This is an invalid schema: `java.lang.AssertionError: Illegal character in: REVENUE$`
While in a database all characters can be used as columns names as long as they are quoted, like `alter table add column "REVENUE$" decimal(12,0);` Avro names are limited to ASCII text, numbers and underscore characters. No slash, backward slash, dollar sign, ...

Granted this does not happen often but if it happens, there must be a way to deal with that. This library provides a schema and column name encoder and decoder.

```
io.rtdi.bigdata.kafka.avro.SchemaNameEncoder {
  public static String encodeName(String name);
  public static String decodeName(String name);
}
```

Essentially it uses "_x<nnnn>" as the encoding pattern, so the encoded name of the column would be `REVENUE_x0024` with 0x24 being the Unicode value for the dollar sign. 
In addition an Avro property is added to the field with the original name as value.

```
    {
      "name":"REVENUE_x0024", ...
      "__originalname" : "REVENUE$"
    }
```

The null-able information is supported by Avro implicitly via an union schema: `"type": ["null", ...]`


## Data types

As seen above, Avro lacks a lot of additional metadata present in the table definition. There is no information about the primary key, the exact data type is lost, even the max length information of a text is not present. In Avro all is just a string, even if it is a single character in a table column.
But for Data Integration this is valuable information. Hence it should be added to Avro as additional property.

Note: The Kafka Key often is the source primary key but must not. The Kafka Key serves more purposes, e.g. partitioning.

```
{
  "name" : "COMPANY_NAME",
  "type" : 
  [ "null", 
    {
      "type" : "string",
      "logicalType" : "NVARCHAR",
      "length" : 30
    }
  ],
  "__originalname" : "COMPANY_NAME"
}
```

This allows Kafka consumers to understand the more detailed data type. Not relevant for Data Lake like consumers, very relevant for database consumers.
This library enables that by adding a more Avro Logical Data Types than what Avro itself provides out of the box, yet remains compatible with all other Avro readers and writers and Kafka products.

To simplify the schema creation, the class SchemaBuilder exists. Above schema with the name encoding and the logical data types plus the primary key information can be built using this code:

```
	SchemaBuilder builder = new SchemaBuilder("CUSTOMER", null);
	builder.add("CUSTOMER_ID", AvroInt.getSchema(), null, false).setPrimaryKey();
	builder.add("COMPANY_NAME", AvroNVarchar.getSchema(30), null, true);
	builder.add("ADDRESS_ID", AvroInt.getSchema(), null, true);
	builder.add("EMPLOYEES", AvroInt.getSchema(), null, true);
	builder.add("REVENUE$", AvroDecimal.getSchema(12, 0), null, true);
	builder.build();
	Schema actualschema = builder.getSchema();
```

## More Metadata

Another type of metadata not present in the default Avro schema is all the information about the producer of the data. In a simple scenario, where there is just one source for an topic, it is clear where the data comes from. But even here it would be interesting to know when the change was captured, part of what transaction, what is the source database row id.
All the information needed for auditing and debugging.

Finally, a change record in the source can be an insert/update/delete (or more) type. Would be interesting to know for the consumer that the row was deleted and not all values updated to null.

To provide a space to store that, the ValueSchema class extends the SchemaBuild with some additional fields.


```
{
  "type" : "record",
  "name" : "CUSTOMER",
  "fields" : [ {
    "name" : "__change_type",
    ...
  }, {
    "name" : "__change_time",
    "type" : {
      "type" : "long",
      "logicalType" : "timestamp-millis"
    },
    ...
  }, {
    "name" : "__source_rowid",
    "type" : [ "null", {
      "type" : "string",
      "logicalType" : "VARCHAR",
      "length" : 30
    } ],
    ...
  }, {
    "name" : "__source_transaction",
    "type" : [ "null", {
      "type" : "string",
      "logicalType" : "VARCHAR",
      "length" : 30
    } ],
    ...
  }, {
    "name" : "__source_system",
    "type" : [ "null", {
      "type" : "string",
      "logicalType" : "VARCHAR",
      "length" : 30
    } ],
    ...
  }, 
  ...
```

The code the create the complete schema is simply using the ValueSchema instead of the SchemaBuilder base class.

```
	ValueSchema value = new ValueSchema("CUSTOMER", null);
	value.add("CUSTOMER_ID", AvroInt.getSchema(), null, false).setPrimaryKey();
	value.add("COMPANY_NAME", AvroNVarchar.getSchema(30), null, true);
	value.add("ADDRESS_ID", AvroInt.getSchema(), null, true);
	value.add("EMPLOYEES", AvroInt.getSchema(), null, true);
	value.add("REVENUE$", AvroDecimal.getSchema(12, 0), null, true);
	value.build();
	Schema actualschema = value.getSchema();
```

The individual fields support some more metadata like the source data type in the source specific language and the content sensitivity to mark field to be excluded for logging its contents or showing the content in the data preview. see `AvroField` methods and the `ContentSensitivity` enum.


## Extensibility concept

A good schema must also provide options for extensions. 
Avro itself does that via the excellent concept of schema evolution, e.g. by adding an additional column.
This is the optimal solution for important fields.
Sometimes we want to augment a record with some additional, probably only technical, information.

An example is that the schema has a field status Completed/Started but the source system is using codes like 1/2 for that. I might be nice to have the official text value but also, for auditing, what the original value was. Clobbering up the schema with all this source specific fields just-in-case does not sound nice. So why not adding an array node `__extension` at every level where such key/value attributes can be stored freely?

```
  {
    "name" : "__extension",
    "type" : [ "null", {
      "type" : "array",
      "items" : {
        "type" : "record",
        "name" : "__extension",
        "doc" : "Extension point to add custom values to each record",
        "fields" : [
           {
	          "name" : "__path",
	          "type" : "string",
	          "doc" : "An unique identifier, e.g. \"street\".\"house number component\""
	        }, {
	          "name" : "__value",
	          "type" : [ "null",
               {"type" : "boolean"},
               {"type" : "bytes"},
               {"type" : "double"},
               {"type" : "float"},
               {"type" : "int"},
               {"type" : "long"},
               {"type" : "string"}
             ],
	          "doc" : "The value of any primitive datatype of Avro"
	        }
	     ]
      }
    } ]
  }
```

When creating the schema via above ValueSchema, the extension node is added automatically at the root level and using ValueSchema.createNewSchema() a nested record with an extension node can be created as well.

## Transformations

Records in a Data Integration process are likely to be transformed as well. This happens in the producer, when data is read from the source and transformed into the Avro schema structure and also within Kafka via e.g. KStreams processes.
Storing the transformations being applied was way too much data for databases. If 1m rows are processed and each records gets 100 transformations applied, that would be 100m rows in the log table. Way too much for a database. But we are not in a database, we use a Big Data tool, Kafka. 
Hence it is time to revisit this point.
Technically this means adding another array at the root level called `__audit` which stores a global state of the record, if the record is to be considered PASS/FAIL/WARN, and an array with the individual transformation steps and their results.


```
__audit (0..1)
+-- __transformationresult (1..1)
+-- __details (0..n)
    +-- __transformationname (1..1)
    +-- __transformationresult (1..1)
    +-- __transformationresulttext (0..1)
    +-- __transformationresultquality (0..1)
```


```
  {
    "name" : "__audit",
    "type" : [ "null", {
      "type" : "record",
      "name" : "__audit",
      "doc" : "If data is transformed this information is recorded here",
      "fields" : [ {
        "name" : "__transformresult",
        "type" : {
          "type" : "string",
          "logicalType" : "VARCHAR",
          "length" : 4
        },
        "doc" : "Is the record PASS, FAILED or WARN?"
      }, {
        "name" : "__details",
        "type" : [ "null", {
          "type" : "array",
          "items" : {
            "type" : "record",
            "name" : "__audit_details",
            "doc" : "Details of all transformations",
            "fields" : [
              {
                 "name" : "__transformationname",
	              "type" : {
	                "type" : "string",
	                "logicalType" : "NVARCHAR",
	                "length" : 1024
	              },
	              "doc" : "A name identifying the applied transformation"
              }, {
	              "name" : "__transformresult",
	              "type" : {
	                "type" : "string",
	                "logicalType" : "VARCHAR",
	                "length" : 4
	              },
	              "doc" : "Is the record PASS, FAIL or WARN?"
	           }, {
	              "name" : "__transformresult_text",
	              "type" : [ "null", {
	                "type" : "string",
	                "logicalType" : "NVARCHAR",
	                "length" : 1024
	              } ],
	              "doc" : "Transforms can optionally describe what they did"
              }, {
	              "name" : "__transformresult_quality",
	              "type" : [ "null", {
	                "type" : "int",
	                "logicalType" : "BYTE"
	              } ],
	              "doc" : "Transforms can optionally return a percent value from 0 (FAIL) to 100 (PASS)"
              }
            ]
          }
        } ],
        "doc" : "Details of all transformations",
        "default" : null
      } ]
    } ],
    "doc" : "If data is transformed this information is recorded here",
    "default" : null
  }
```

The ValueSchema contains this optional structure, the `RuleResult.aggregate()` helps combining the individual rule results into a global one.
The logic is quite simple:

* PASS + PASS = PASS
* PASS + WARN = WARN
* WARN + WARN = WARN
* xxxx + FAIL = FAIL

In other words, the global result is the minimum of the individual results.