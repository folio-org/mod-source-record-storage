-- delete "matchedProfileId" field
UPDATE ${myuniversity}_${mymodule}.records
SET jsonb = jsonb - 'matchedProfileId';
