package io.rtdi.bigdata.kafka.avro;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.rtdi.bigdata.kafka.avro.objects.ImpactLineage;
import io.rtdi.bigdata.kafka.avro.objects.ImpactLineage.SourceTable;
import io.rtdi.bigdata.kafka.avro.objects.ImpactLineage.TargetTable;

/**
 * Test all operations around ImpactLineage documents
 */
public class ImpactLineageTest {

	/**
	 * Constructor for the test class
	 */
	public ImpactLineageTest() {
	}

	/**
	 * Prepares the test environment before any tests are run.
	 * @throws Exception if something goes wrong
	 */
	@BeforeAll
	public static void setUp() throws Exception {
	}

	/**
	 * Tears down the test environment after all tests are run.
	 * @throws Exception if something goes wrong
	 */
	@AfterAll
	public static void tearDown() throws Exception {
	}

	/**
	 * Run the tests.
	 */
	@Test
	public void test() {
		try {
			ImpactLineage il = new ImpactLineage("function1", "df1");
			TargetTable targettable = il.addTargetTable("t1", "c1");
			SourceTable sourceTable = new SourceTable("s1", "c2", "copy", "straight copy");
			sourceTable.setKey("uuid1");
			targettable.addSourceTable(sourceTable);
			targettable.addOneToOneMapping(sourceTable, "c1", "c1", "1:1");


			String json = il.toRecordJson();
			GenericData.Record triggerrecord = il.toRecord();
			ImpactLineage il2 = ImpactLineage.fromRecordJson(json);
			ImpactLineage il3 = ImpactLineage.from(triggerrecord);

			assertEquals(il2, il);
			assertEquals(il3, il2);

			Path path = Path.of("src/test/resources", "il_values.json");
			Files.writeString(path, json);

			ObjectMapper om = new ObjectMapper();

			JsonNode json_tree = om.readTree(json);
			path = Path.of("src/test/resources/expected", "il_values.json");
			JsonNode expected_tree = om.readTree(path.toFile());
			assertEquals(expected_tree, json_tree, "The built object is different from the expected object");

		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

}
