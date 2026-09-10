import unittest

from python.src.kafkaavro.avro_datatypes_pydantic import AvroNVarchar, AvroInt, ArraySchema, AvroDate, RecordSchema, \
    AvroDouble
from python.src.kafkaavro.schemabuilder_pydantic import ValueSchema
import json


class SchemaCreation(unittest.TestCase):

    def test_create(self):
        value_schema = ValueSchema(name="Schema1", namespace=None)
        value_schema.add_field("PKCOL1", AvroNVarchar(length=10), nullable=False)
        value_schema.add_field("An/Avron&unsupported$Columnname", AvroInt(), nullable=True)
        value_schema.add_field("ARRAY1", ArraySchema(items=AvroDate()), nullable=True)
        nested_record = RecordSchema(name="nested_schema1", namespace=None)
        nested_record.add_field("NS1", AvroDouble(), nullable=True)
        nested_record.add_field("NS2", AvroDouble(), nullable=True)
        value_schema.add_field("nested_record", nested_record, nullable=True)
        value_schema.add_field("children", ArraySchema(items=nested_record), nullable=True)
        value_schema.set_pks({"PKCOL1",})

        json_str = value_schema.get_avro_json()
        json_tree = json.loads(json_str)
        with open("../../src/test/resources/schema1.avsc") as file:
            expected_str = file.read()
        expected_tree = json.loads(expected_str)
        self.assertEquals(expected_tree, json_tree, "The built schema is different from the expected schema")

        value_schema2 = ValueSchema.from_avro_schema(json_str)
        self.assertEquals(value_schema, value_schema2)



if __name__ == '__main__':
    unittest.main()

