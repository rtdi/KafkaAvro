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

import io.rtdi.bigdata.kafka.avro.datatypes.AvroDecimal;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroInt;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroNVarchar;
import io.rtdi.bigdata.kafka.avro.datatypes.ColumnSemantics;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroField.ColumnType;
import io.rtdi.bigdata.kafka.avro.objects.Duration;
import io.rtdi.bigdata.kafka.avro.objects.TableSemantics;
import io.rtdi.bigdata.kafka.avro.objects.TableType;
import io.rtdi.bigdata.kafka.avro.objects.TimeUnit;
import io.rtdi.bigdata.kafka.avro.recordbuilders.ValueSchema;

/**
 * Some tests to create a schema
 */
public class SchemaCreationSampleTest {
	private ObjectMapper om = AvroUtils.createJacksonOM();

	/**
	 * Constructor for the test class
	 */
	public SchemaCreationSampleTest() {
	}

	/**
	 * prepare the test environment
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

	@Test
	/**
	 * Create a schema for a CUSTOMER table
	 */
	public void test() {
		try {
			ValueSchema valueschema = new ValueSchema("CUSTOMER", null);
			valueschema.add("CUSTOMER_ID", AvroInt.create(), null, false);
			valueschema.add("COMPANY_NAME", AvroNVarchar.create(30), null, true).setSemantics(new ColumnSemantics(ColumnType.TEXT));
			valueschema.add("ADDRESS_ID", AvroInt.create(), null, true);
			valueschema.add("EMPLOYEES", AvroInt.create(), null, true);
			valueschema.add("REVENUE$", AvroDecimal.create(12, 0), null, true);
			valueschema.add("GBU", AvroNVarchar.create(10), null, true);
			valueschema.setPrimaryKeys("CUSTOMER_ID");
			valueschema.addForeignKey("Customer to Address", "ADDRESS", "ADDRESS_ID", "ADDRESS_ID", "=");
			valueschema.setDataProductOwner("owner@company.com");
			valueschema.setRetentionPeriod(new Duration(6, TimeUnit.YEARS));
			valueschema.setRegulations("GDPR", "EAR");
			valueschema.setObjectLevelSecurity("group1", "group2");
			valueschema.addRowLevelSecurity("GBU", "GBU");
			valueschema.setSemantics(new TableSemantics(TableType.FACT));

			Files.createDirectories(Path.of("src/test/resources"));

			// save its Avro Schema Json as file
			{
				String json = valueschema.toAvroJson();
				Path path = Path.of("src/test/resources", "customer.avsc");
				Files.writeString(path, json);

				JsonNode json_tree = om.readTree(json);
				path = Path.of("src/test/resources/expected", "customer.avsc");
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
				Path path = Path.of("src/test/resources", "customer.json");
				Files.writeString(path, json);

				JsonNode json_tree = om.readTree(json);
				path = Path.of("src/test/resources/expected", "customer.json");
				JsonNode expected_tree = om.readTree(path.toFile());
				assertEquals(expected_tree, json_tree, "The built object schema is different from the expected schema");

				ValueSchema valueschema2 = ValueSchema.fromObjectJson(json);
				assertEquals(valueschema, valueschema2);
			}

		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

}

