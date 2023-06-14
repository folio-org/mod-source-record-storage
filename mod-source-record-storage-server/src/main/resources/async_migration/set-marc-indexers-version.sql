-- delete marc_indexers related to OLD records
DELETE FROM marc_indexers
WHERE exists(
  SELECT 1
  FROM records_lb
  WHERE records_lb.id = marc_indexers.marc_id
    AND records_lb.state = 'OLD'
);

-- set marc_indexers version, populate marc_records_tracking table and create indexes on marc_indexers table
do $$
  declare
    index integer;
    suffix text;
  begin
    execute 'update marc_indexers set version = 0;';
    execute 'insert into marc_records_tracking select id, 0, false from marc_records_lb;';
    for index in 0 .. 999 loop
      suffix = lpad(index::text, 3, '0');
      execute 'drop index if exists idx_marc_indexers_marc_id_' || suffix || ';';
      execute 'create index idx_marc_indexers_marc_id_version_' || suffix || ' on marc_indexers_' || suffix || '(marc_id, version);';
    end loop;
  end;
$$;

ALTER TABLE marc_indexers ALTER COLUMN version SET NOT NULL;
CREATE INDEX IF NOT EXISTS idx_marc_records_tracking_dirty ON marc_records_tracking USING btree (is_dirty);
