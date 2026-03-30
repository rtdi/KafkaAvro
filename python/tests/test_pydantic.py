import json
import unittest

import python.src.kafkaavro.avro_datatypes_pydantic as dtpd
import python.src.kafkaavro.avro_datatypes as dt
import python.src.kafkaavro.schemabuilder_pydantic as sbpd
import python.src.kafkaavro.schemabuilder as sb

schema_json = """
{
    "__originalname": "RESULT",
    "data_classifications": null,
    "data_product_owner_email": null,
    "deletion_policy": null,
    "fields": [
        {
            "__internal": true,
            "__originalname": "__audit",
            "__sensitivity": "INTERNAL",
            "__source_data_type": null,
            "__technical": true,
            "default": null,
            "name": "__audit",
            "type": [
                "null",
                {
                    "__originalname": "__audit",
                    "fields": [
                        {
                            "__internal": false,
                            "__originalname": "__transformresult",
                            "__sensitivity": "INTERNAL",
                            "__source_data_type": null,
                            "__technical": false,
                            "default": null,
                            "doc": "Is the record PASS, FAIL or WARN?",
                            "name": "__transformresult",
                            "type": [
                                "null",
                                {
                                    "length": 4,
                                    "logicalType": "VARCHAR",
                                    "type": "string"
                                }
                            ]
                        },
                        {
                            "__internal": false,
                            "__originalname": "__details",
                            "__sensitivity": "INTERNAL",
                            "__source_data_type": null,
                            "__technical": false,
                            "default": null,
                            "doc": "Details of all transformations",
                            "name": "__details",
                            "type": [
                                "null",
                                {
                                    "items": {
                                        "__originalname": "__audit_details",
                                        "fields": [
                                            {
                                                "__internal": false,
                                                "__originalname": "__transformationname",
                                                "__sensitivity": "INTERNAL",
                                                "__source_data_type": null,
                                                "__technical": false,
                                                "default": null,
                                                "doc": "A name identifying the applied transformation",
                                                "name": "__transformationname",
                                                "type": [
                                                    "null",
                                                    {
                                                        "length": 1024,
                                                        "logicalType": "NVARCHAR",
                                                        "type": "string"
                                                    }
                                                ]
                                            },
                                            {
                                                "__internal": false,
                                                "__originalname": "__transformresult",
                                                "__sensitivity": "INTERNAL",
                                                "__source_data_type": null,
                                                "__technical": false,
                                                "default": null,
                                                "doc": "Is the record PASS, FAIL or WARN?",
                                                "name": "__transformresult",
                                                "type": [
                                                    "null",
                                                    {
                                                        "length": 4,
                                                        "logicalType": "VARCHAR",
                                                        "type": "string"
                                                    }
                                                ]
                                            },
                                            {
                                                "__internal": false,
                                                "__originalname": "__transformresult_text",
                                                "__sensitivity": "INTERNAL",
                                                "__source_data_type": null,
                                                "__technical": false,
                                                "default": null,
                                                "doc": "Transforms can optionally describe what they did",
                                                "name": "__transformresult_text",
                                                "type": [
                                                    "null",
                                                    {
                                                        "length": 1024,
                                                        "logicalType": "NVARCHAR",
                                                        "type": "string"
                                                    }
                                                ]
                                            },
                                            {
                                                "__internal": false,
                                                "__originalname": "__transformresult_quality",
                                                "__sensitivity": "INTERNAL",
                                                "__source_data_type": null,
                                                "__technical": false,
                                                "default": null,
                                                "doc": "Transforms can optionally return a percent value from 0 (FAIL) to 100 (PASS)",
                                                "name": "__transformresult_quality",
                                                "type": [
                                                    "null",
                                                    {
                                                        "logicalType": "BYTE",
                                                        "type": "int"
                                                    }
                                                ]
                                            }
                                        ],
                                        "name": "__audit_details",
                                        "type": "record"
                                    },
                                    "type": "array"
                                }
                            ]
                        }
                    ],
                    "name": "__audit",
                    "type": "record"
                }
            ]
        },
        {
            "__internal": true,
            "__originalname": "__change_type",
            "__sensitivity": "INTERNAL",
            "__source_data_type": null,
            "__technical": true,
            "default": null,
            "doc": "Indicates how the row is to be processed: Insert, Update, Delete, upsert/Autocorrect, eXterminate, Truncate,...",
            "name": "__change_type",
            "type": [
                "null",
                {
                    "length": 1,
                    "logicalType": "VARCHAR",
                    "type": "string"
                }
            ]
        },
        {
            "__internal": true,
            "__originalname": "__change_time",
            "__sensitivity": "INTERNAL",
            "__source_data_type": null,
            "__technical": true,
            "default": null,
            "doc": "Timestamp of the transaction. All rows of the transaction have the same value.",
            "name": "__change_time",
            "type": [
                "null",
                {
                    "logicalType": "timestamp-millis",
                    "type": "long"
                }
            ]
        },
        {
            "__internal": true,
            "__originalname": "__source_rowid",
            "__sensitivity": "INTERNAL",
            "__source_data_type": null,
            "__technical": true,
            "default": null,
            "doc": "Optional unique and static pointer to the row, e.g. Oracle rowid",
            "name": "__source_rowid",
            "type": [
                "null",
                {
                    "length": 30,
                    "logicalType": "VARCHAR",
                    "type": "string"
                }
            ]
        },
        {
            "__internal": true,
            "__originalname": "__source_transaction",
            "__sensitivity": "INTERNAL",
            "__source_data_type": null,
            "__technical": true,
            "default": null,
            "doc": "Optional source transaction information for auditing",
            "name": "__source_transaction",
            "type": [
                "null",
                {
                    "length": 30,
                    "logicalType": "VARCHAR",
                    "type": "string"
                }
            ]
        },
        {
            "__internal": true,
            "__originalname": "__source_system",
            "__sensitivity": "INTERNAL",
            "__source_data_type": null,
            "__technical": true,
            "default": null,
            "doc": "Optional source system information for auditing",
            "name": "__source_system",
            "type": [
                "null",
                {
                    "length": 30,
                    "logicalType": "VARCHAR",
                    "type": "string"
                }
            ]
        },
        {
            "__internal": true,
            "__originalname": "__truncate",
            "__sensitivity": "INTERNAL",
            "__source_data_type": null,
            "__technical": true,
            "default": null,
            "doc": "In case of a change type of TRUNCATE, this map contains the fields to identify the set of rows to be deleted",
            "name": "__truncate",
            "type": [
                "null",
                {
                    "logicalType": "MAP",
                    "type": "map",
                    "values": {
                        "logicalType": "STRING",
                        "type": "string"
                    }
                }
            ]
        },
        {
            "__internal": true,
            "__originalname": "__extension",
            "__sensitivity": "INTERNAL",
            "__source_data_type": null,
            "__technical": false,
            "default": null,
            "doc": "Add more columns beyond the official logical data model",
            "name": "__extension",
            "type": [
                "null",
                {
                    "__originalname": "__extension",
                    "doc": "Extension point to add custom values to each record",
                    "fields": [
                        {
                            "__internal": false,
                            "__originalname": "__path",
                            "__sensitivity": "INTERNAL",
                            "__source_data_type": null,
                            "__technical": false,
                            "doc": "An unique identifier",
                            "name": "__path",
                            "type": {
                                "logicalType": "STRING",
                                "type": "string"
                            }
                        },
                        {
                            "__internal": false,
                            "__originalname": "__value",
                            "__sensitivity": "INTERNAL",
                            "__source_data_type": null,
                            "__technical": false,
                            "doc": "The value of any primitive datatype of Avro",
                            "name": "__value",
                            "type": {
                                "logicalType": "STRING",
                                "type": "string"
                            }
                        }
                    ],
                    "name": "__extension",
                    "type": "record"
                }
            ]
        }
    ],
    "fks": null,
    "name": "RESULT",
    "namespace": "LABWARE",
    "pks": null,
    "repo_url": null,
    "retention_period": null,
    "tickets_url": null,
    "type": "record"
}
"""

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


    def test_schema_registry(self):
        schema_dict = json.loads(schema_json)
        rec = sbpd.ValueSchema(**schema_dict)
        print(rec.get_schema_name())
        print(rec.model_dump_json())


if __name__ == '__main__':
    unittest.main()