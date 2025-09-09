package io.rtdi.bigdata.kafka.avro;

import static org.junit.jupiter.api.Assertions.fail;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.SchemaFormatter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.rtdi.bigdata.kafka.avro.datatypes.AvroDecimal;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroInt;
import io.rtdi.bigdata.kafka.avro.datatypes.AvroNVarchar;
import io.rtdi.bigdata.kafka.avro.recordbuilders.Duration;
import io.rtdi.bigdata.kafka.avro.recordbuilders.FKCondition;
import io.rtdi.bigdata.kafka.avro.recordbuilders.TimeUnit;
import io.rtdi.bigdata.kafka.avro.recordbuilders.ValueSchema;

/**
 * Some tests to create a schema
 */
public class SchemaCreationSampleTest {

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
	 * Create a schema for a CUSTOMER table
	 */
	@Test
	public void test() {
		try {
			ValueSchema value = new ValueSchema("CUSTOMER", null);
			value.add("CUSTOMER_ID", AvroInt.getSchema(), null, false);
			value.add("COMPANY_NAME", AvroNVarchar.getSchema(30), null, true);
			value.add("ADDRESS_ID", AvroInt.getSchema(), null, true);
			value.add("EMPLOYEES", AvroInt.getSchema(), null, true);
			value.add("REVENUE$", AvroDecimal.getSchema(12, 0), null, true);
			value.setPrimaryKey("CUSTOMER_ID");
			value.addForeignKey("Customer to Address", "ADDRESS", "ADDRESS_ID", "ADDRESS_ID", "=");
			value.setDataProductOwner("owner@company.com");
			value.setRetentionPeriod(new Duration(6, TimeUnit.YEARS));
			value.setRegulations("GDPR", "EAR");
			value.build();
			Schema actualschema = value.getSchema();
			String schema_text = SchemaFormatter.format("json/pretty", actualschema);
			System.out.println(schema_text);
			Files.createDirectories(Path.of("src/test/resources"));
			Path path = Path.of("src/test/resources", "customer.avsc");
			Files.writeString(path, schema_text);

			List<FKCondition> fks = value.getForeignKeys();
			System.out.println("Foreign Keys: " + fks);
			List<String> pks = value.getPrimaryKeys();
			System.out.println("Primary Keys: " + pks);

		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

}

