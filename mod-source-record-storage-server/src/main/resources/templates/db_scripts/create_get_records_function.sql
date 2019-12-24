CREATE OR REPLACE FUNCTION get_records(where_clause text, order_by text, limitVal int, offsetVal int, schema_name text)
  RETURNS TABLE (id uuid, jsonb json, totalrows bigint)
    AS $$
BEGIN
  RETURN query
     EXECUTE format('
            SELECT records.id,
                   json_build_object(''id'', records.jsonb->>''id'',
                        ''snapshotId'', records.jsonb->>''snapshotId'',
                        ''matchedProfileId'', records.jsonb->>''matchedProfileId'',
                        ''matchedId'', records.jsonb->>''matchedId'',
                        ''generation'', (records.jsonb->>''generation'')::integer,
                        ''recordType'', records.jsonb->>''recordType'',
                        ''deleted'', records.jsonb->>''deleted'',
                        ''order'', (records.jsonb->>''order'')::integer,
                        ''externalIdsHolder'', records.jsonb->''externalIdsHolder'',
                        ''additionalInfo'', records.jsonb->''additionalInfo'',
                        ''metadata'', records.jsonb->''metadata'',
                        ''rawRecord'', raw_records.jsonb,
                        ''parsedRecord'', COALESCE(marc_records.jsonb),
                        ''errorRecord'', error_records.jsonb)
                        AS jsonb,
                   (SELECT COUNT(id) FROM %s.records
                    %s
                   ) AS totalrows
            FROM %s.records
            JOIN %s.raw_records ON records.jsonb->>''rawRecordId'' = raw_records.jsonb->>''id''
            LEFT JOIN %s.marc_records ON records.jsonb->>''parsedRecordId'' = marc_records.jsonb->>''id''
            LEFT JOIN %s.error_records ON records.jsonb->>''errorRecordId'' = error_records.jsonb->>''id''
			      %s
			      %s
            LIMIT %s OFFSET %s',
            schema_name, where_clause, schema_name, schema_name, schema_name, schema_name, where_clause, order_by, limitVal, offsetVal);
END $$
language plpgsql;


CREATE OR REPLACE FUNCTION get_source_records(query_filter text, order_by text, limitVal int, offsetVal int, deleted_records text, schema_name text)
  RETURNS TABLE (id uuid, jsonb json, totalrows bigint)
    AS $$
BEGIN
  RETURN query
     EXECUTE format('
            SELECT records.id,
                   json_build_object(''recordId'', records.jsonb->>''id'',
                            ''snapshotId'', records.jsonb->>''snapshotId'',
                            ''recordType'', records.jsonb->>''recordType'',
                            ''deleted'', records.jsonb->>''deleted'',
                            ''order'', records.jsonb->>''order'',
                            ''additionalInfo'', records.jsonb->''additionalInfo'',
                            ''metadata'', records.jsonb->''metadata'',
                            ''rawRecord'', raw_records.jsonb,
                            ''parsedRecord'', COALESCE(marc_records.jsonb))
                            AS jsonb,
                   (SELECT COUNT(id) FROM %s.records
                    WHERE records.jsonb->>''parsedRecordId'' IS NOT NULL
                      AND records.jsonb->>''deleted'' = ''%s''
                      %s
                    ) AS totalrows
            FROM %s.records
            JOIN %s.raw_records ON records.jsonb->>''rawRecordId'' = raw_records.jsonb->>''id''
            LEFT JOIN %s.marc_records ON records.jsonb->>''parsedRecordId'' = marc_records.jsonb->>''id''
            WHERE records.jsonb->>''parsedRecordId'' IS NOT NULL
              AND records.jsonb->>''deleted'' = ''%s''
			        %s
			      %s
            LIMIT %s OFFSET %s',
            schema_name, deleted_records, query_filter, schema_name, schema_name, schema_name, deleted_records, query_filter, order_by, limitVal, offsetVal);
END $$
language plpgsql;
