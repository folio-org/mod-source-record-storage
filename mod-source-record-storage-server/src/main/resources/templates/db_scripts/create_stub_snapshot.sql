INSERT INTO ${myuniversity}_${mymodule}.snapshots (_id, jsonb) values
('00000000-0000-0000-0000-000000000000', '{
    "jobExecutionId": "00000000-0000-0000-0000-000000000000",
    "status": "COMMITTED"
  }') ON CONFLICT DO NOTHING;
