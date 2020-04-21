UPDATE ${myuniversity}_${mymodule}.records
SET jsonb = jsonb_set(records.jsonb, '{state}', '"ACTUAL"', true)
WHERE (records.jsonb->>'state') IS NULL OR TRIM(coalesce(records.jsonb->>'state', '')) = ''::text;


