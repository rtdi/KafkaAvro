import unittest

import python.src.kafkaavro.avro_datatypes_pydantic as dtpd
import python.src.kafkaavro.avro_datatypes as dt
import python.src.kafkaavro.schemabuilder_pydantic as sbpd
import python.src.kafkaavro.schemabuilder as sb


class SchemaTests(unittest.TestCase):

    def test_name_encoding(self):
        rec1 = sb.ValueSchema(name="rec/1", namespace=None)
        rec1.add_field("f1", dt.AvroBoolean(), nullable=True)
        rec1.add_field("f$2", dt.AvroUnion(items=[dt.AvroBoolean(), dt.AvroString()]), nullable=False)
        rec1.add_pk("f1")
        rec2 = sbpd.ValueSchema(name="rec/1", namespace=None)
        rec2.add_field("f1", dtpd.AvroBoolean(), nullable=True)
        rec2.add_field("f$2", dtpd.AvroUnion(root=[dtpd.AvroBoolean(), dtpd.AvroString()]), nullable=False)
        rec2.add_pk("f1")
        d1 = rec1.create_schema_dict()
        d2 = rec2.model_dump()
        print(d1)
        print(d2)
        self.assertEqual(d1, d2)
        rec3 = sbpd.ValueSchema.model_validate(d1)
        print(rec3)
        json2 = rec2.get_json()
        json3 = rec3.get_json()
        print(json2)
        print(json3)
        self.assertEqual(json2, json3)

        print(rec1.get_schema_name())
        print(rec3.get_schema_name())
        self.assertEqual(rec1.get_schema_name(), rec3.get_schema_name())

        key = sbpd.KeySchema(rec2)
        print(key.get_json())

if __name__ == '__main__':
    unittest.main()