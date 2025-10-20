import unittest
import avro.schema

from python.src.kafkaavro.avro_datatypes import (AvroDecimal, AvroInt, AvroNVarchar, AvroVarchar, AvroEnum, AvroFixed, \
    AvroMap, AvroTime, AvroTimeMicros, AvroDate, AvroTimestamp, AvroTimestampMicros, get_datatype, \
    encode_name, decode_name)
from python.src.kafkaavro.data_governance import TimeUnit, FKCondition, Duration
from python.src.kafkaavro.impact_lineage import ImpactLineage, SourceTable, impact_lineage_value_schema, \
    impact_lineage_key_schema
from python.src.kafkaavro.schemabuilder import ValueSchema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter


class SchemaTests(unittest.TestCase):

    def test_schema_create(self):
        value = ValueSchema("CUSTOMER", None)
        value.add_field("CUSTOMER_ID", AvroInt(), None, False)
        value.add_field("COMPANY_NAME", AvroNVarchar(30), None, True)
        value.add_field("ADDRESS_ID", AvroInt(), None, True)
        value.add_field("EMPLOYEES", AvroInt(), None, True)
        value.add_field("REVENUE$", AvroDecimal(12, 0), None, True)
        value.set_pks({"CUSTOMER_ID"})
        value.add_fk(FKCondition("Customer to Address", "ADDRESS", "ADDRESS_ID", "ADDRESS_ID", "="))
        value.set_data_product_owner_email("owner@company.com")
        value.set_retention_period(Duration(6, TimeUnit.YEARS))
        value.add_data_classifications("GDPR")
        value.add_data_classifications("EAR")
        schema_str = value.get_json()
        print(schema_str)
        schema = avro.schema.parse(schema_str)
        with open("customer.avsc", "w") as f:
            f.write(schema_str)


    def test_name_encoding(self):
        name = "A complicated $ name with german äöü umlaut"
        encoded = encode_name(name)
        decoded = decode_name(encoded)
        assert decoded, name


    def test_impact_lineage(self):
        il = ImpactLineage("p1", "df1")
        s1 = SourceTable("s1", "db2", "copy")
        t1 = il.add_target_table("t1", "db1")
        t1.add_source_table(s1)
        t1.add_1_to_1_mapping(s1, "source_col_1", "target_col_1")
        il_value_schema = impact_lineage_value_schema
        il_key_schema = impact_lineage_key_schema
        schema_str = il_value_schema.get_json()
        print(schema_str)
        schema = avro.schema.parse(schema_str)
        with open("impact_lineage.avsc", "w") as f:
            f.write(schema_str)
        d = il.create_dict()
        writer = DataFileWriter(open("test.avro", "wb"), DatumWriter(), schema)
        writer.append(d)
        writer.close()

    def test_schema_conversion(self):
        value = ValueSchema("ALL_DATA_TYPES", None)
        value.add_field("D_INT", AvroInt())
        value.add_field("D_NVARCHAR", AvroNVarchar(30))
        value.add_field("D_VARCHAR", AvroVarchar(10))
        value.add_field("D_ENUM", AvroEnum(["FIRST", "SECOND"]))
        value.add_field("D_DECIMAL", AvroDecimal(12, 0))
        value.add_field("D_FIXED", AvroFixed(10))
        value.add_field("D_MAP", AvroMap(AvroInt()))
        value.add_field("D_TIME", AvroTime())
        value.add_field("D_TIME_MICRO", AvroTimeMicros())
        value.add_field("D_DATE", AvroDate())
        value.add_field("D_TIMESTAMP", AvroTimestamp())
        value.add_field("D_TIMESTAMP_MICROS", AvroTimestampMicros())
        d = value.create_schema_dict()
        value2 = get_datatype(d)


if __name__ == '__main__':
    unittest.main()