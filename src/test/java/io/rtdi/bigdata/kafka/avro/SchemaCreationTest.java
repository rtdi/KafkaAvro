package io.rtdi.bigdata.kafka.avro;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.avro.Schema;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.rtdi.bigdata.kafka.avro.datatypes.AvroDate;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroDouble;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroInt;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroNVarchar;
import io.rtdi.bigdata.kafka.avro.recordbuilders.SchemaBuilder;
import io.rtdi.bigdata.kafka.avro.recordbuilders.ValueSchema;

/**
 * Some basic tests to create a schema
 */
public class SchemaCreationTest {

	private static final String expectedschemajson = "{\"type\":\"record\",\"name\":\"Schema1\",\"fields\":[{\"name\":\"__change_type\",\"type\":{\"type\":\"string\",\"logicalType\":\"VARCHAR\",\"length\":1},\"doc\":\"Indicates how the row is to be processed: Insert, Update, Delete, upsert/Autocorrect, eXterminate, Truncate,...\",\"default\":\"UPSERT\",\"__originalname\":\"__change_type\",\"__internal\":true,\"__technical\":true},{\"name\":\"__truncate\",\"type\":{\"type\":\"map\",\"values\":{\"type\":\"string\",\"logicalType\":\"STRING\"},\"logicalType\":\"MAP\"},\"doc\":\"In case of a change type of TRUNCATE, this map contains the fields to identify the set of rows to be deleted\",\"__originalname\":\"__truncate\"},{\"name\":\"__change_time\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"},\"doc\":\"Timestamp of the transaction. All rows of the transaction have the same value.\",\"default\":0,\"__originalname\":\"__change_time\",\"__internal\":true,\"__technical\":true},{\"name\":\"__source_rowid\",\"type\":[\"null\",{\"type\":\"string\",\"logicalType\":\"VARCHAR\",\"length\":30}],\"doc\":\"Optional unqiue and static pointer to the row, e.g. Oracle rowid\",\"__originalname\":\"__source_rowid\",\"__internal\":true,\"__technical\":true},{\"name\":\"__source_transaction\",\"type\":[\"null\",{\"type\":\"string\",\"logicalType\":\"VARCHAR\",\"length\":30}],\"doc\":\"Optional source transaction information for auditing\",\"__originalname\":\"__source_transaction\",\"__internal\":true,\"__technical\":true},{\"name\":\"__source_system\",\"type\":[\"null\",{\"type\":\"string\",\"logicalType\":\"VARCHAR\",\"length\":30}],\"doc\":\"Optional source system information for auditing\",\"__originalname\":\"__source_system\",\"__internal\":true,\"__technical\":true},{\"name\":\"__extension\",\"type\":[\"null\",{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"__extension\",\"doc\":\"Extension point to add custom values to each record\",\"fields\":[{\"name\":\"__path\",\"type\":{\"type\":\"string\",\"logicalType\":\"STRING\"},\"doc\":\"An unique identifier, e.g. \\\"street\\\".\\\"house number component\\\"\",\"__originalname\":\"__path\"},{\"name\":\"__value\",\"type\":[\"null\",{\"type\":\"boolean\",\"logicalType\":\"BOOLEAN\"},{\"type\":\"bytes\",\"logicalType\":\"BYTES\"},{\"type\":\"double\",\"logicalType\":\"DOUBLE\"},{\"type\":\"float\",\"logicalType\":\"FLOAT\"},{\"type\":\"int\",\"logicalType\":\"INT\"},{\"type\":\"long\",\"logicalType\":\"LONG\"},{\"type\":\"string\",\"logicalType\":\"STRING\"}],\"doc\":\"The value of any primitive datatype of Avro\",\"__originalname\":\"__value\"}],\"__originalname\":\"__extension\"}}],\"doc\":\"Add more columns beyond the official logical data model\",\"default\":null,\"__originalname\":\"__extension\",\"__internal\":true},{\"name\":\"__audit\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"__audit\",\"doc\":\"If data is transformed this information is recorded here\",\"fields\":[{\"name\":\"__transformresult\",\"type\":{\"type\":\"string\",\"logicalType\":\"VARCHAR\",\"length\":4},\"doc\":\"Is the record PASS, FAILED or WARN?\",\"__originalname\":\"__transformresult\"},{\"name\":\"__details\",\"type\":[\"null\",{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"__audit_details\",\"doc\":\"Details of all transformations\",\"fields\":[{\"name\":\"__transformationname\",\"type\":{\"type\":\"string\",\"logicalType\":\"NVARCHAR\",\"length\":1024},\"doc\":\"A name identifying the applied transformation\",\"__originalname\":\"__transformationname\"},{\"name\":\"__transformresult\",\"type\":{\"type\":\"string\",\"logicalType\":\"VARCHAR\",\"length\":4},\"doc\":\"Is the record PASS, FAIL or WARN?\",\"__originalname\":\"__transformresult\"},{\"name\":\"__transformresult_text\",\"type\":[\"null\",{\"type\":\"string\",\"logicalType\":\"NVARCHAR\",\"length\":1024}],\"doc\":\"Transforms can optionally describe what they did\",\"__originalname\":\"__transformresult_text\"},{\"name\":\"__transformresult_quality\",\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"BYTE\"}],\"doc\":\"Transforms can optionally return a percent value from 0 (FAIL) to 100 (PASS)\",\"__originalname\":\"__transformresult_quality\"}],\"__originalname\":\"__audit_details\"}}],\"doc\":\"Details of all transformations\",\"default\":null,\"__originalname\":\"__details\"}],\"__originalname\":\"__audit\"}],\"doc\":\"If data is transformed this information is recorded here\",\"default\":null,\"__originalname\":\"__audit\",\"__internal\":true},{\"name\":\"PKCOL1\",\"type\":{\"type\":\"string\",\"logicalType\":\"NVARCHAR\",\"length\":10},\"__originalname\":\"PKCOL1\"},{\"name\":\"An_x002fAvron_x0026unsupported_x0024Columnname\",\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"INT\"}],\"__originalname\":\"An/Avron&unsupported$Columnname\"},{\"name\":\"ARRAY1\",\"type\":[\"null\",{\"type\":\"array\",\"items\":{\"type\":\"int\",\"logicalType\":\"date\"}}],\"doc\":\"Array of dates\",\"default\":null,\"__originalname\":\"ARRAY1\"},{\"name\":\"nested_record\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"nested_schema1\",\"fields\":[{\"name\":\"N1\",\"type\":[\"null\",{\"type\":\"double\",\"logicalType\":\"DOUBLE\"}],\"__originalname\":\"N1\"},{\"name\":\"N2\",\"type\":[\"null\",{\"type\":\"double\",\"logicalType\":\"DOUBLE\"}],\"__originalname\":\"N2\"}],\"__originalname\":\"nested_schema1\"}],\"default\":null,\"__originalname\":\"nested_record\"},{\"name\":\"children\",\"type\":[\"null\",{\"type\":\"array\",\"items\":\"nested_schema1\"}],\"default\":null,\"__originalname\":\"children\"}],\"__originalname\":\"Schema1\",\"pks\":[\"PKCOL1\"],\"fks\":null}";

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
	 * Tessts
	 */
	@Test
	public void test() {
		try {
			ValueSchema builder = new ValueSchema("Schema1", null);
			builder.add("PKCOL1", AvroNVarchar.getSchema(10), null, false);
			builder.add("An/Avron&unsupported$Columnname", AvroInt.getSchema(), null, true);
			builder.addColumnArray("ARRAY1", AvroDate.getSchema(), "Array of dates");
			SchemaBuilder nested_record_builder = new SchemaBuilder("nested_schema1", null);
			nested_record_builder.add("N1", AvroDouble.getSchema(), null, true);
			nested_record_builder.add("N2", AvroDouble.getSchema(), null, true);
			builder.addColumnRecord("nested_record", nested_record_builder, null, true);
			builder.addColumnRecordArray("children", nested_record_builder, null);
			builder.setPrimaryKey("PKCOL1");
			builder.build();
			Schema actualschema = builder.getSchema();
			Schema expectedschema = new Schema.Parser().parse(expectedschemajson);
			assertEquals(expectedschema, actualschema, "The built schema is different from the expected schema");
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

}
