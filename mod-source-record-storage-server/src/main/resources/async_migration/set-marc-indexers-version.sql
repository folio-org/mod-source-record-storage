-- set marc_indexers version, populate marc_records_tracking table and create indexes on marc_indexers table
do $$
  declare
    index integer;
    suffix text;
  begin
    execute 'update marc_indexers set version = 0 where version IS NULL;';
    execute 'insert into marc_records_tracking ' ||
            'select id, 0, false ' ||
            'from marc_records_lb ' ||
            'left join marc_records_tracking ON marc_records_tracking.marc_id = marc_records_lb.id ' ||
            'where marc_records_tracking.marc_id IS NULL;';
    for index in 0 .. 999 loop
      suffix = lpad(index::text, 3, '0');
      execute 'drop index if exists idx_marc_indexers_marc_id_' || suffix || ';';
      execute 'create index if not exists idx_marc_indexers_marc_id_version_' || suffix || ' on marc_indexers_' || suffix || '(marc_id, version);';
    end loop;
  end;
$$;

ALTER TABLE marc_indexers ALTER COLUMN version SET NOT NULL;
CREATE INDEX IF NOT EXISTS idx_marc_records_tracking_dirty ON marc_records_tracking USING btree (is_dirty);
