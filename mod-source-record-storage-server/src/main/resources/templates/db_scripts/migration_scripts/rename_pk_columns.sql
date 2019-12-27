-- Scripts rename all columns with name "_id" to "id"
DO $$ BEGIN IF EXISTS(SELECT * FROM information_schema.columns WHERE table_name='snapshots' AND column_name='_id') THEN ALTER TABLE ${myuniversity}_${mymodule}.snapshots RENAME COLUMN _id TO id; END IF; END $$;
DO $$ BEGIN IF EXISTS(SELECT * FROM information_schema.columns WHERE table_name='error_records' AND column_name='_id') THEN ALTER TABLE ${myuniversity}_${mymodule}.error_records RENAME COLUMN _id TO id; END IF; END $$;
DO $$ BEGIN IF EXISTS(SELECT * FROM information_schema.columns WHERE table_name='marc_records' AND column_name='_id') THEN ALTER TABLE ${myuniversity}_${mymodule}.marc_records RENAME COLUMN _id TO id; END IF; END $$;
DO $$ BEGIN IF EXISTS(SELECT * FROM information_schema.columns WHERE table_name='raw_records' AND column_name='_id') THEN ALTER TABLE ${myuniversity}_${mymodule}.raw_records RENAME COLUMN _id TO id; END IF; END $$;
DO $$ BEGIN IF EXISTS(SELECT * FROM information_schema.columns WHERE table_name='records' AND column_name='_id') THEN ALTER TABLE ${myuniversity}_${mymodule}.records RENAME COLUMN _id TO id; END IF; END $$;

