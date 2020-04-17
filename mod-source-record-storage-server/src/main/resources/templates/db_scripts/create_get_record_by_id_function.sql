
CREATE OR REPLACE FUNCTION get_record_by_matched_id(record_id uuid)
RETURNS json AS $recordDto$
DECLARE
	recordDto json;
BEGIN
  SELECT json_build_object('id', records.jsonb->>'id',
          'snapshotId', records.jsonb->>'snapshotId',
					'matchedProfileId', records.jsonb->>'matchedProfileId',
					'matchedId', records.jsonb->>'matchedId',
					'generation', (records.jsonb->>'generation')::integer,
					'recordType', records.jsonb->>'recordType',
					'deleted', records.jsonb->>'deleted',
					'order', (records.jsonb->>'order')::integer,
					'externalIdsHolder', records.jsonb->'externalIdsHolder',
					'additionalInfo', records.jsonb->'additionalInfo',
					'metadata', records.jsonb->'metadata',
					'state', records.jsonb->'state',
 					'rawRecord', raw_records.jsonb,
 					'parsedRecord', COALESCE(marc_records.jsonb),
					'errorRecord', error_records.jsonb)
					AS jsonb
      INTO recordDto
  FROM records
  JOIN raw_records ON records.jsonb->>'rawRecordId' = raw_records.jsonb->>'id'
  LEFT JOIN marc_records ON records.jsonb->>'parsedRecordId' = marc_records.jsonb->>'id'
  LEFT JOIN error_records ON records.jsonb->>'errorRecordId' = error_records.jsonb->>'id'
  WHERE (records.jsonb ->> 'matchedId')::uuid = record_id
  AND records.jsonb->>'state' = 'ACTUAL';

RETURN recordDto;
END;
$recordDto$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_source_record_by_id(record_id uuid)
RETURNS json AS $sourceRecordDto$
DECLARE
	sourceRecordDto json;
BEGIN
  SELECT json_build_object('recordId', records.jsonb->>'matchedId',
          'snapshotId', records.jsonb->>'snapshotId',
					'recordType', records.jsonb->>'recordType',
					'deleted', records.jsonb->>'deleted',
					'order', (records.jsonb->>'order')::integer,
					'externalIdsHolder', records.jsonb->'externalIdsHolder',
					'additionalInfo', records.jsonb->'additionalInfo',
					'metadata', records.jsonb->'metadata',
 					'rawRecord', raw_records.jsonb,
 					'parsedRecord', COALESCE(marc_records.jsonb))
					AS jsonb
      INTO sourceRecordDto
  FROM records
  JOIN raw_records ON records.jsonb->>'rawRecordId' = raw_records.jsonb->>'id'
  LEFT JOIN marc_records ON records.jsonb->>'parsedRecordId' = marc_records.jsonb->>'id'
  WHERE (records.jsonb ->> 'matchedId')::uuid = record_id
  AND records.jsonb->>'state' = 'ACTUAL'
  AND records.jsonb->>'deleted' = 'false'
  AND records.jsonb->>'parsedRecordId' IS NOT NULL;

RETURN sourceRecordDto;
END;
$sourceRecordDto$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION get_source_record_by_external_id(externalId uuid, idFieldName text)
RETURNS json AS $sourceRecordDto$
DECLARE
	sourceRecordDto json;
BEGIN
  SELECT json_build_object('recordId', records.jsonb->>'matchedId',
                    'snapshotId', records.jsonb->>'snapshotId',
					'recordType', records.jsonb->>'recordType',
					'deleted', records.jsonb->>'deleted',
					'order', (records.jsonb->>'order')::integer,
					'additionalInfo', records.jsonb->'additionalInfo',
					'metadata', records.jsonb->'metadata',
 					'rawRecord', raw_records.jsonb,
 					'externalIdsHolder', records.jsonb->'externalIdsHolder',
 					'parsedRecord', COALESCE(marc_records.jsonb))
					AS jsonb
      INTO sourceRecordDto
  FROM records
  JOIN raw_records ON records.jsonb->>'rawRecordId' = raw_records.jsonb->>'id'
  LEFT JOIN marc_records ON records.jsonb->>'parsedRecordId' = marc_records.jsonb->>'id'
  WHERE (records.jsonb -> 'externalIdsHolder' ->> idFieldName)::uuid = externalId
  AND records.jsonb->>'deleted' = 'false'
  AND records.jsonb->>'parsedRecordId' IS NOT NULL
  AND records.jsonb->>'state' = 'ACTUAL';

RETURN sourceRecordDto;
END;
$sourceRecordDto$ LANGUAGE plpgsql;
