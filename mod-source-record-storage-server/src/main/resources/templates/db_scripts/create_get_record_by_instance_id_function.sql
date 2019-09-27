-- Custom script to create a function to get the MARC record by instanceId. Changes in this file will not result in an update of the function.
-- To change the function, update this script and copy it to the appropriate scripts.snippet field of the schema.json

CREATE OR REPLACE FUNCTION get_record_by_instance_id(instanceId uuid, externalIdType text)
RETURNS jsonb AS $recordByInstId$
DECLARE
	recordByInstId jsonb;
	idFieldName text;
BEGIN
  CASE
    WHEN externalIdType = 'INSTANCE' THEN idFieldName := 'instanceId';
  END CASE;

  SELECT json_build_object('id', recordsByExternalId.jsonb->>'id',
            'snapshotId', recordsByExternalId.jsonb->>'snapshotId',
            'matchedProfileId', recordsByExternalId.jsonb->>'matchedProfileId',
            'matchedId', recordsByExternalId.jsonb->>'matchedId',
            'generation', recordsByExternalId.jsonb->>'generation',
            'recordType', recordsByExternalId.jsonb->>'recordType',
            'deleted', recordsByExternalId.jsonb->>'deleted',
            'additionalInfo', recordsByExternalId.jsonb->'additionalInfo',
            'metadata', recordsByExternalId.jsonb->'metadata',
            'rawRecord', raw_records.jsonb,
            'parsedRecord', COALESCE(marc_records.jsonb),
            'errorRecord', error_records.jsonb)
            INTO recordByInstId
    FROM (SELECT * FROM records
          WHERE (jsonb -> 'externalIdsHolder' ->> idFieldName)::uuid = instanceId
            AND (jsonb ->> 'generation')::int = (SELECT MAX((jsonb ->> 'generation')::int) FROM records)) AS recordsByExternalId
    JOIN raw_records ON recordsByExternalId.jsonb->>'rawRecordId' = raw_records.jsonb->>'id'
    LEFT JOIN marc_records ON recordsByExternalId.jsonb->>'parsedRecordId' = marc_records.jsonb->>'id'
    LEFT JOIN error_records ON recordsByExternalId.jsonb->>'errorRecordId' = error_records.jsonb->>'id';

  RETURN recordByInstId;
END;
$recordByInstId$ LANGUAGE plpgsql;
