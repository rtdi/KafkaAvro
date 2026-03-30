import json
import unittest
import avro.schema

import python.src.kafkaavro.avro_datatypes as ad
import python.src.kafkaavro.data_governance as dg
import python.src.kafkaavro.schemabuilder as sb
import python.src.kafkaavro.avro_datatypes_pydantic as adp
import python.src.kafkaavro.data_governance_pydantic as dgp
import python.src.kafkaavro.schemabuilder_pydantic as sbp
from avro.datafile import DataFileWriter
from avro.io import DatumWriter


class SchemaTests(unittest.TestCase):

    def test_schema_create(self):
        value = sb.ValueSchema(name="CUSTOMER", namespace=None)
        value.add_field("CUSTOMER_ID", ad.AvroInt(), None, False)
        value.add_field("COMPANY_NAME", ad.AvroNVarchar(length=30), None, True)
        value.add_field("ADDRESS_ID", ad.AvroInt(), None, True)
        value.add_field("EMPLOYEES", ad.AvroInt(), None, True)
        value.add_field("REVENUE$", ad.AvroDecimal(precision=12, scale=0), None, True)
        value.set_pks({"CUSTOMER_ID"})
        value.add_fk(sb.FKCondition(fk_name="Customer to Address", fk_schema_fqn="ADDRESS", left_field_name="ADDRESS_ID", right_field_name="ADDRESS_ID", condition="="))
        value.set_data_product_owner_email("owner@company.com")
        value.set_retention_period(dg.Duration(value=6, unit=dg.TimeUnit.YEARS.name))
        value.add_data_classifications("GDPR")
        value.add_data_classifications("EAR")
        schema_str = value.get_json()
        schema = avro.schema.parse(schema_str)
        with open("customer.avsc", "w") as f:
            f.write(schema_str)

        value3 = sbp.ValueSchema(name="CUSTOMER", namespace=None)
        value3.add_field("CUSTOMER_ID", adp.AvroInt(), None, False)
        value3.add_field("COMPANY_NAME", adp.AvroNVarchar(length=30), None, True)
        value3.add_field("ADDRESS_ID", adp.AvroInt(), None, True)
        value3.add_field("EMPLOYEES", adp.AvroInt(), None, True)
        value3.add_field("REVENUE$", adp.AvroDecimal(precision=12, scale=0), None, True)
        value3.set_pks({"CUSTOMER_ID"})
        value3.add_fk(sbp.FKCondition(fk_name="Customer to Address", fk_schema_fqn="ADDRESS", conditions=[dgp.JoinCondition(left_field_name="ADDRESS_ID", right_field_name="ADDRESS_ID", condition="=")]))
        value3.set_data_product_owner_email("owner@company.com")
        value3.set_retention_period(dgp.Duration(value=6, unit=dg.TimeUnit.YEARS.name))
        value3.add_data_classifications("GDPR")
        value3.add_data_classifications("EAR")
        schema_str3 = value3.model_dump_json()
        schema3 = avro.schema.parse(schema_str3)

        j = json.loads(schema_str)
        j3 = json.loads(schema_str3)
        self.assertCountEqual(j, j3)


    def test_name_encoding(self):
        name = "A complicated $ name with german äöü umlaut"
        encoded = ad.encode_name(name)
        decoded = ad.decode_name(encoded)
        self.assertEqual(decoded, name)


    # def test_impact_lineage(self):
    #     il = ImpactLineage("p1", "df1")
    #     s1 = SourceTable("s1", "db2", "copy")
    #     t1 = il.add_target_table("t1", "db1")
    #     t1.add_source_table(s1)
    #     t1.add_1_to_1_mapping(s1, "source_col_1", "target_col_1")
    #     il_value_schema = impact_lineage_value_schema
    #     il_key_schema = impact_lineage_key_schema
    #     schema_str = il_value_schema.get_json()
    #     print(schema_str)
    #     schema = avro.schema.parse(schema_str)
    #     with open("impact_lineage.avsc", "w") as f:
    #         f.write(schema_str)
    #     d = il.create_dict()
    #     writer = DataFileWriter(open("test.avro", "wb"), DatumWriter(), schema)
    #     writer.append(d)
    #     writer.close()

    def test_schema_conversion(self):
        value = sb.ValueSchema(name="ALL_DATA_TYPES", namespace=None)
        value.add_field("D_INT", ad.AvroInt())
        value.add_field("D_NVARCHAR", ad.AvroNVarchar(length=30))
        value.add_field("D_VARCHAR", ad.AvroVarchar(length=10))
        value.add_field("D_ENUM", ad.AvroEnum(symbols=["FIRST", "SECOND"], name="enum_1"))
        value.add_field("D_DECIMAL", ad.AvroDecimal(precision=12, scale=0))
        value.add_field("D_FIXED", ad.AvroFixed(length=10, name="fixed_1"))
        value.add_field("D_MAP", ad.AvroMap(value_data_type=ad.AvroInt()))
        value.add_field("D_TIME", ad.AvroTime())
        value.add_field("D_TIME_MICRO", ad.AvroTimeMicros())
        value.add_field("D_DATE", ad.AvroDate())
        value.add_field("D_TIMESTAMP", ad.AvroTimestamp())
        value.add_field("D_TIMESTAMP_MICROS", ad.AvroTimestampMicros())
        d = value.get_json()

        value3 = sbp.ValueSchema(name="ALL_DATA_TYPES", namespace=None)
        value3.add_field("D_INT", adp.AvroInt())
        value3.add_field("D_NVARCHAR", adp.AvroNVarchar(length=30))
        value3.add_field("D_VARCHAR", adp.AvroVarchar(length=10))
        value3.add_field("D_ENUM", adp.AvroEnum(symbols=["FIRST", "SECOND"], name="enum_1"))
        value3.add_field("D_DECIMAL", adp.AvroDecimal(precision=12, scale=0))
        value3.add_field("D_FIXED", adp.AvroFixed(size=10, name="fixed_1"))
        value3.add_field("D_MAP", adp.AvroMap(values=adp.AvroInt()))
        value3.add_field("D_TIME", adp.AvroTime())
        value3.add_field("D_TIME_MICRO", adp.AvroTimeMicros())
        value3.add_field("D_DATE", adp.AvroDate())
        value3.add_field("D_TIMESTAMP", adp.AvroTimestamp())
        value3.add_field("D_TIMESTAMP_MICROS", adp.AvroTimestampMicros())
        d3 = value3.get_json()

        j = json.loads(d)
        j3 = json.loads(d3)
        self.assertCountEqual(j, j3)
        value2 = sbp.ValueSchema.model_validate_json(d3)
        self.assertCountEqual(value3.model_dump(), value2.model_dump())


if __name__ == '__main__':
    unittest.main()