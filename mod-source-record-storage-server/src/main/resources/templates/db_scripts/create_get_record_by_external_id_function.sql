-- Custom script to create a function to get the MARC record by externalId. Changes in this file will not result in an update of the function.
-- To change the function, update this script and copy it to the appropriate scripts.snippet field of the schema.json

CREATE OR REPLACE FUNCTION get_record_by_external_id(externalId uuid, idFieldName text)
RETURNS jsonb AS $recordDtoByExternalId$
DECLARE
	recordDtoByExternalId jsonb;
BEGIN
  SELECT json_build_object('id', recordsByExternalId.jsonb->>'id',
            'snapshotId', recordsByExternalId.jsonb->>'snapshotId',
            'matchedId', recordsByExternalId.jsonb->>'matchedId',
            'generation', recordsByExternalId.jsonb->>'generation',
            'recordType', recordsByExternalId.jsonb->>'recordType',
            'deleted', recordsByExternalId.jsonb->>'deleted',
            'externalIdsHolder', recordsByExternalId.jsonb->'externalIdsHolder',
            'additionalInfo', recordsByExternalId.jsonb->'additionalInfo',
            'metadata', recordsByExternalId.jsonb->'metadata',
            'rawRecord', raw_records.jsonb,
            'parsedRecord', COALESCE(marc_records.jsonb),
            'errorRecord', error_records.jsonb)
          INTO recordDtoByExternalId
  FROM (SELECT rcd.*
        FROM (
              SELECT rc.*, max(generation) over (partition by matchedId) max_generation
              FROM (
                    SELECT r.*,
                           (r.jsonb ->> 'matchedId')::text  matchedId,
                           (r.jsonb ->> 'generation')::text generation
                    FROM records r
                    WHERE (jsonb -> 'externalIdsHolder' ->> idFieldName)::uuid = externalId) rc) rcd
          WHERE generation = max_generation) recordsByExternalId
          JOIN raw_records ON recordsByExternalId.jsonb ->> 'rawRecordId' = raw_records.jsonb ->> 'id'
          LEFT JOIN marc_records ON recordsByExternalId.jsonb ->> 'parsedRecordId' = marc_records.jsonb ->> 'id'
          LEFT JOIN error_records ON recordsByExternalId.jsonb ->> 'errorRecordId' = error_records.jsonb ->> 'id';

  RETURN recordDtoByExternalId;
END;
$recordDtoByExternalId$ LANGUAGE plpgsql;
