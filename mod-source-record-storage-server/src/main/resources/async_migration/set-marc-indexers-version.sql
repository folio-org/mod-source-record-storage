do $$
  declare
    index integer;
    suffix text;
  begin
    -- Update in smaller batches to reduce lock contention
    perform 'update marc_indexers set version = 0 where version IS NULL LIMIT 1000;';

    -- Insert in smaller batches to reduce lock contention
    perform 'insert into marc_records_tracking ' ||
            'select id, 0, false ' ||
            'from marc_records_lb ' ||
            'left join marc_records_tracking ON marc_records_tracking.marc_id = marc_records_lb.id ' ||
            'where marc_records_tracking.marc_id IS NULL LIMIT 1000;';

    -- Create indexes in smaller batches or defer until other operations complete
    for index in 0 .. 999 loop
      suffix = lpad(index::text, 3, '0');
      execute 'drop index if exists idx_marc_indexers_marc_id_' || suffix || ';';
      execute 'create index if not exists idx_marc_indexers_marc_id_version_' || suffix || ' on marc_indexers_' || suffix || '(marc_id, version);';
    end loop;
  end;
$$;

-- Add lock timeout to avoid long waits on locks
SET lock_timeout = '5s';
ALTER TABLE marc_indexers ALTER COLUMN version SET NOT NULL;
CREATE INDEX IF NOT EXISTS idx_marc_records_tracking_dirty ON marc_records_tracking USING btree (is_dirty);
