CREATE EXTENSION IF NOT EXISTS moddatetime;

DO $$ BEGIN
	CREATE TYPE cron_job_status AS ENUM ('PENDING', 'SUCCESS', 'FAILED');
EXCEPTION
WHEN duplicate_object THEN NULL;
END $$;


CREATE TABLE IF NOT EXISTS cron_jobs (
	id uuid default gen_random_uuid(), -- int key does not play well in distributed environment.

	name text not null,
	type text not null,
	data jsonb not null,
	status cron_job_status not null default 'PENDING',
	failure_reason text check ((status = 'FAILED') = (failure_reason IS NOT NULL)),

	scheduled_at timestamptz not null,

	created_at timestamptz not null default now(),
	updated_at timestamptz not null default now(),

	primary key (id),
	unique (name)
);

CREATE OR REPLACE TRIGGER staged_jobs_moddatetime
	BEFORE UPDATE ON cron_jobs
	FOR EACH ROW
	EXECUTE PROCEDURE moddatetime(updated_at);
