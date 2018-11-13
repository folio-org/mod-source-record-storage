-- Custom script to create records_view. Changes in this file will not result in an update of the view.
-- To change the view, update this script and copy it to the scripts.snippet field of the schema.json

CREATE OR REPLACE VIEW records_view AS
  SELECT records._id,
  json_build_object('id', records.jsonb->>'id',
          'snapshotId', records.jsonb->>'snapshotId',
					'matchedProfileId', records.jsonb->>'matchedProfileId',
					'matchedId', records.jsonb->>'matchedId',
					'generation', records.jsonb->>'generation',
					'recordType', records.jsonb->>'recordType',
 					'sourceRecord', source_records.jsonb,
 					'parsedRecord', COALESCE(marc_records.jsonb),
					'errorRecord', error_records.jsonb)
					AS jsonb
   FROM records
     JOIN source_records ON records.jsonb->>'sourceRecordId' = source_records.jsonb->>'id'
     LEFT JOIN marc_records ON records.jsonb->>'parsedRecordId' = marc_records.jsonb->>'id'
     LEFT JOIN error_records ON records.jsonb->>'errorRecordId' = error_records.jsonb->>'id';

-- to add a table that stores another type of parsed record,
-- add LEFT JOIN on that table and its jsonb field as a param of COALESCE, for example:
--
--CREATE OR REPLACE VIEW records_view AS
--  SELECT records._id,
--  json_build_object('id', records.jsonb->>'id',
--          'snapshotId', records.jsonb->>'snapshotId',
--					'matchedProfileId', records.jsonb->>'matchedProfileId',
--					'matchedId', records.jsonb->>'matchedId',
--					'generation', records.jsonb->>'generation',
--					'recordType', records.jsonb->>'recordType',
-- 					'sourceRecord', source_records.jsonb,
-- 					'parsedRecord', COALESCE(marc_records.jsonb, new_type_of_parsed_records.jsonb),
--					'errorRecord', error_records.jsonb)
--					AS jsonb
--   FROM records
--     JOIN source_records ON records.jsonb->>'sourceRecordId' = source_records.jsonb->>'id'
--     LEFT JOIN marc_records ON records.jsonb->>'parsedRecordId' = marc_records.jsonb->>'id'
--     LEFT JOIN error_records ON records.jsonb->>'errorRecordId' = error_records.jsonb->>'id'
--     LEFT JOIN new_type_of_parsed_records ON records.jsonb->>'parsedRecordId' = new_type_of_parsed_records.jsonb->>'id';
