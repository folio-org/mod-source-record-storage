INSERT INTO ${myuniversity}_${mymodule}.snapshots (id, jsonb) values
('00000000-0000-0000-0000-000000000000', '{
    "jobExecutionId": "00000000-0000-0000-0000-000000000000",
    "status": "COMMITTED",
    "processingStartedDate": "2019-01-01T12:00:00.000"
  }') ON CONFLICT DO NOTHING;
