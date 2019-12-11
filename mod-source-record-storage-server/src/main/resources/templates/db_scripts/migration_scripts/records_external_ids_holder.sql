UPDATE ${myuniversity}_${mymodule}.records
SET	jsonb = jsonb_set(records.jsonb, '{externalIdsHolder}', jsonb_build_object('instanceId', marc_record_instance_id.instance_id))
FROM (
	SELECT marc_fields._id, subfields -> 'i' as instance_id
	FROM (
		SELECT marc_records._id, marc_records.jsonb, fields
		FROM ${myuniversity}_${mymodule}.marc_records
		CROSS JOIN LATERAL jsonb_array_elements(jsonb -> 'content' -> 'fields') AS fields
		WHERE fields ? '999'
	) AS marc_fields
	CROSS JOIN LATERAL jsonb_array_elements(fields -> '999' -> 'subfields') AS subfields
	WHERE subfields ? 'i'
) AS marc_record_instance_id
WHERE (records.jsonb ->> 'parsedRecordId')::uuid = marc_record_instance_id._id;
