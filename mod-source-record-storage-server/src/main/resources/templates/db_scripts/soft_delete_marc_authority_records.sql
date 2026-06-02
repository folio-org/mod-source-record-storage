-- Script for https://folio-org.atlassian.net/browse/MODSOURCE-999
-- Description:
-- This script soft-deletes MARC Authority records whose associated
-- snapshot is in ERROR status and whose external authority record does not
-- exist in mod-entities-links.
--
-- It performs the following actions:
-- 1. Identifies MARC_AUTHORITY records in ACTUAL state with ERROR snapshot
--    that have no corresponding record in the authority table.
-- 2. Updates records_lb: sets state to DELETED, suppress_discovery to true,
--    and updated_date to the current timestamp.
-- 3. Updates marc_records_lb content (JSONB):
--    - Sets leader position 05 (record status) to 'd' (deleted).
--    - Updates the 005 control field to the current date/time (YYYYMMDDHHMMSS.0).
--
-- Requirements:
-- Replace ${tenantId} placeholder with specific tenant id
SET search_path TO ${tenantId}_mod_source_record_storage, ${tenantId}_mod_entities_links;
WITH targets AS (
    SELECT r.id
    FROM records_lb AS r
    JOIN snapshots_lb AS s
      ON s.id = r.snapshot_id
    WHERE r.record_type = 'MARC_AUTHORITY'
      AND r.state       = 'ACTUAL'
      AND s.status      = 'ERROR'
      AND NOT EXISTS (
            SELECT 1
            FROM authority AS a
            WHERE a.id = r.external_id
          )
),
updated_records AS (
    UPDATE records_lb AS r
    SET state              = 'DELETED',
        suppress_discovery = true,
        updated_date       = now()
    FROM targets t
    WHERE r.id = t.id
    RETURNING r.id
)
UPDATE marc_records_lb AS mr
SET content = jsonb_set(
        jsonb_set(
            mr.content,
            '{leader}',
            to_jsonb(overlay(mr.content->>'leader' placing 'd' from 6 for 1))
        ),
        '{fields}',
        (
            SELECT jsonb_agg(
                CASE WHEN elem ? '005'
                     THEN jsonb_build_object('005', to_char(now(), 'YYYYMMDDHH24MISS') || '.0')
                     ELSE elem
                END
            )
            FROM jsonb_array_elements(mr.content->'fields') elem
        )
    )
FROM updated_records u
WHERE mr.id = u.id;
