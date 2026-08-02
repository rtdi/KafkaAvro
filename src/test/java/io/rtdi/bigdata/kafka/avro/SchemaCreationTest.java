package io.rtdi.bigdata.kafka.avro;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.avro.Schema;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.rtdi.bigdata.kafka.avro.datatypes.AvroArray;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroDate;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroDouble;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroInt;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroNVarchar;
import io.rtdi.bigdata.kafka.avro.datatypes.RecordSchema;
import io.rtdi.bigdata.kafka.avro.recordbuilders.ValueSchema;

/**
 * Some basic tests to create a schema
 */
public class SchemaCreationTest {
	private ObjectMapper om = AvroUtils.createJacksonOM();

	/**
	 * Prepare the test environment
	 * @throws Exception if something goes wrong
	 */
	@BeforeAll
	public static void setUp() throws Exception {
	}

	/**
	 * tears down the test environment after all tests are run.
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
			// Build a schema manually
			ValueSchema valueschema = new ValueSchema("Schema1", null);
			valueschema.add("PKCOL1", AvroNVarchar.create(10), null, false);
			valueschema.add("An/Avron&unsupported$Columnname", AvroInt.create(), null, true);
			valueschema.add("ARRAY1", new AvroArray(AvroDate.create()), "Array of dates", true);
			RecordSchema nested_record_builder = new RecordSchema("nested_schema1", null);
			nested_record_builder.add("N1", AvroDouble.create(), null, true);
			nested_record_builder.add("N2", AvroDouble.create(), null, true);
			valueschema.add("nested_record", nested_record_builder, null, true);
			valueschema.add("children", new AvroArray(nested_record_builder), null, true);
			valueschema.setPrimaryKeys("PKCOL1");

			Files.createDirectories(Path.of("src/test/resources"));

			// save its Avro Schema Json as file
			{
				String json = valueschema.toAvroJson();
				Path path = Path.of("src/test/resources", "schema1.avsc");
				Files.writeString(path, json);

				JsonNode json_tree = om.readTree(json);
				path = Path.of("src/test/resources/expected", "schema1.avsc");
				JsonNode expected_tree = om.readTree(path.toFile());
				assertEquals(expected_tree, json_tree, "The built schema is different from the expected schema");

				Schema avroschema = valueschema.createSchema();

				// Create a ValueSchema from an Avro Schema
				ValueSchema valueschema2 = new ValueSchema(avroschema);
				assertEquals(valueschema, valueschema2);
			}

			// Test the object json format
			{
				String json = valueschema.toObjectJson();
				Path path = Path.of("src/test/resources", "schema1.json");
				Files.writeString(path, json);

				JsonNode json_tree = om.readTree(json);
				path = Path.of("src/test/resources/expected", "schema1.json");
				JsonNode expected_tree = om.readTree(path.toFile());
				assertEquals(expected_tree, json_tree, "The built object schema is different from the expected schema");

				ValueSchema valueschema2 = ValueSchema.fromObjectJson(json);
				assertEquals(valueschema, valueschema2);
			}

		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

}
