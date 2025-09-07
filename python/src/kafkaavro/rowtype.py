from enum import Enum

class RowType(str, Enum):
    INSERT="I"
    # A brand new record was inserted. A record with this primary key was not present before.
    # If there is no guarantee such record does not exist yet, use UPSERT instead.

    UPDATE="U"
    # An existing record was updated.

    BEFORE="B"
    # The update's before image. This is useful in case the primary key was changed.
    
    DELETE="D"
    # An existing record was deleted, the provided records contains the complete latest version with all payload fields.
    # If only the primary key of the payload is known, use EXTEMRINATE instead.
    
    UPSERT="A"
    # In case either a new record should be created or its last version overwritten, use this UPSERT RowType ("AutoCorrect").
    
    EXTERMINATE="X"
    # When the payload of a delete has null values everywhere except for the primary key fields, then the proper code is EXTERMINATE.
    # A database would execute a "delete from table where pk = ?" and ignore all other fields.
    
    TRUNCATE="T"
    # Delete a set of rows at once. An example could be to delete all records of a given patient from the diagnosis table.
    # In that case the diagnosis table would get a record of type truncate with all payload fields including the PK being null, only the patient field has a value.
    
    REPLACE="R"
    # A TRUNCATE followed by the new rows. Example could be a case where all data of a patient should be reloaded.
    # A TRUNCATE row would be sent to all tables to remove the data and all new data is inserted. But to indicate that this was done via a truncate-replace, the
    # rows are not flagged as INSERT but REPLACE.
    #
    # Note that an UPSERT would not work in such scenarios as a patient might have had 10 diagnosis rows but meanwhile just 9. The UPSERT would not modify
    # record #10, the truncate on the other hand deletes all 10 records and re-inserts 9 records.

    ARCHIVE="P"
    # Marks a record as archived. The record was deleted in the source database, but should probably remain in the target.