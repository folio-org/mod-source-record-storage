-- Script for https://folio-org.atlassian.net/browse/MODSOURCE-999
-- Description:
-- This script hard-deletes MARC Authority records whose external authority record
-- does not exist in mod-entities-links.
--
-- Requirements:
-- Replace ${tenantId} placeholder with specific tenant id
SET search_path TO ${tenantId}_mod_source_record_storage, ${tenantId}_mod_entities_links;
DELETE FROM records_lb r
WHERE r.record_type = 'MARC_AUTHORITY'
  AND r.external_id IS NOT NULL
  AND NOT EXISTS (
        SELECT 1
        FROM authority a
        WHERE a.id = r.external_id
      );
