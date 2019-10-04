-- Custom script to create records_view. Changes in this file will not result in an update of the view.
-- To change the view, update this script and copy it to the appropriate scripts.snippet field of the schema.json

CREATE OR REPLACE VIEW records_view AS
  SELECT records._id,
  json_build_object('id', records.jsonb->>'id',
          'snapshotId', records.jsonb->>'snapshotId',
					'matchedProfileId', records.jsonb->>'matchedProfileId',
					'matchedId', records.jsonb->>'matchedId',
					'generation', records.jsonb->>'generation',
					'recordType', records.jsonb->>'recordType',
					'deleted', records.jsonb->>'deleted',
					'order', records.jsonb->>'order',
					'externalIdsHolder', records.jsonb->'externalIdsHolder',
					'additionalInfo', records.jsonb->'additionalInfo',
					'metadata', records.jsonb->'metadata',
 					'rawRecord', raw_records.jsonb,
 					'parsedRecord', COALESCE(marc_records.jsonb),
					'errorRecord', error_records.jsonb)
					AS jsonb
   FROM records
     JOIN raw_records ON records.jsonb->>'rawRecordId' = raw_records.jsonb->>'id'
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
--          'metadata', records.jsonb->'metadata',
-- 					'rawRecord', raw_records.jsonb,
-- 					'parsedRecord', COALESCE(marc_records.jsonb, new_type_of_parsed_records.jsonb),
--					'errorRecord', error_records.jsonb)
--					AS jsonb
--   FROM records
--     JOIN raw_records ON records.jsonb->>'rawRecordId' = raw_records.jsonb->>'id'
--     LEFT JOIN marc_records ON records.jsonb->>'parsedRecordId' = marc_records.jsonb->>'id'
--     LEFT JOIN error_records ON records.jsonb->>'errorRecordId' = error_records.jsonb->>'id'
--     LEFT JOIN new_type_of_parsed_records ON records.jsonb->>'parsedRecordId' = new_type_of_parsed_records.jsonb->>'id';
--
--Similarly to update source_records_view the following script has to be updated and copied to the appropriate scripts.snippet field of the schema.json
CREATE OR REPLACE VIEW source_records_view AS
  SELECT records._id,
  json_build_object('recordId', records.jsonb->>'id',
          'snapshotId', records.jsonb->>'snapshotId',
					'recordType', records.jsonb->>'recordType',
					'deleted', records.jsonb->>'deleted',
					'order', records.jsonb->>'order',
					'additionalInfo', records.jsonb->'additionalInfo',
					'metadata', records.jsonb->'metadata',
					'rawRecord', raw_records.jsonb,
 					'parsedRecord', COALESCE(marc_records.jsonb))
					AS jsonb
   FROM records
     JOIN raw_records ON records.jsonb->>'rawRecordId' = raw_records.jsonb->>'id'
     LEFT JOIN marc_records ON records.jsonb->>'parsedRecordId' = marc_records.jsonb->>'id'
     WHERE records.jsonb->>'parsedRecordId' IS NOT NULL;


CREATE INDEX records_raw_record_id_idx_btree ON records USING BTREE ((jsonb ->> 'rawRecordId'));
CREATE INDEX records_parsed_record_id_idx_btree ON records USING BTREE ((jsonb ->> 'parsedRecordId'));
CREATE INDEX raw_records_id_idx_btree ON raw_records USING BTREE ((jsonb ->> 'id'));
CREATE INDEX marc_records_id_idx_btree ON marc_records USING BTREE ((jsonb ->> 'id'));
CREATE INDEX error_records_id_idx_btree ON error_records USING BTREE ((jsonb ->> 'id'));
