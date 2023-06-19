-- delete marc_indexers related to OLD records
DELETE FROM marc_indexers
WHERE exists(
  SELECT 1
  FROM records_lb
  WHERE records_lb.id = marc_indexers.marc_id
    AND records_lb.state = 'OLD'
);
