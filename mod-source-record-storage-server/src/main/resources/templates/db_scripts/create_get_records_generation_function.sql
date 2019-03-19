-- Custom script to create a function to get the highest generation for the record. Changes in this file will not result in an update of the function.
-- To change the function, update this script and copy it to the appropriate scripts.snippet field of the schema.json

CREATE OR REPLACE FUNCTION get_highest_generation(matchedId uuid, snapshotId uuid)
RETURNS integer AS $generation$
DECLARE
	generation integer;
BEGIN
SELECT MAX(records.jsonb ->> 'generation') into generation
	FROM records
	INNER JOIN snapshots ON records.jsonb ->> 'snapshotId' = snapshots.jsonb ->> 'jobExecutionId'
	WHERE (records.jsonb ->> 'matchedId')::uuid = matchedId
 	  AND snapshots.jsonb ->> 'status' = 'COMMITTED'
    AND (snapshots.jsonb -> 'metadata' ->> 'updatedDate')::timestamp with time zone <
      (SELECT snapshots.jsonb ->> 'processingStartedDate'
	      FROM snapshots
	      WHERE (snapshots.jsonb ->> 'jobExecutionId')::uuid = snapshotId)::timestamp with time zone;
RETURN generation;
END;
$generation$ LANGUAGE plpgsql;
