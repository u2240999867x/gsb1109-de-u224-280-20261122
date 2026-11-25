-- Rolling warehouse API v1 -> v2 support for APScheduler 4.
--
-- Run as a database owner/administrator after APScheduler has created its
-- tables. Replace the role names/passwords to match deployment. The old
-- process needs no code or add_job() change: PostgreSQL Row Level Security
-- makes v2 and unresolved rows invisible to its existing queries.

BEGIN;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'warehouse_old') THEN
        CREATE ROLE warehouse_old LOGIN PASSWORD 'change-me-old';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'warehouse_new') THEN
        CREATE ROLE warehouse_new LOGIN PASSWORD 'change-me-new';
    END IF;
END
$$;

ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS protocol text,
    ADD COLUMN IF NOT EXISTS routing_status text NOT NULL DEFAULT 'ready',
    ADD COLUMN IF NOT EXISTS routing_labels jsonb NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS classification_reason text;

ALTER TABLE jobs
    DROP CONSTRAINT IF EXISTS jobs_routing_status_check;
ALTER TABLE jobs
    ADD CONSTRAINT jobs_routing_status_check
    CHECK (routing_status IN ('ready', 'manual_review'));

CREATE INDEX IF NOT EXISTS ix_jobs_warehouse_claim
    ON jobs (created_at)
    WHERE routing_status = 'ready';

CREATE TABLE IF NOT EXISTS warehouse_manual_review (
    job_id uuid PRIMARY KEY REFERENCES jobs(id) ON DELETE CASCADE,
    status text NOT NULL DEFAULT 'awaiting_manual_verification',
    reason text NOT NULL,
    batch_number text,
    original_args bytea,
    original_kwargs bytea,
    labels jsonb NOT NULL DEFAULT '[]'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    checked_at timestamptz,
    CHECK (status IN ('awaiting_manual_verification', 'confirmed', 'rejected'))
);

CREATE TABLE IF NOT EXISTS warehouse_receipts (
    job_id uuid PRIMARY KEY,
    batch_number text NOT NULL UNIQUE,
    input_amount integer NOT NULL,
    protocol_used text NOT NULL CHECK (protocol_used IN ('v1', 'v2')),
    amount_display text NOT NULL,
    currency text NOT NULL DEFAULT 'EUR',
    worker_id text,
    receipt jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_warehouse_receipts_expires
    ON warehouse_receipts (expires_at);

CREATE OR REPLACE FUNCTION warehouse_classify_job()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    labels jsonb := '[]'::jsonb;
    explicit text;
    old_roles text[] := ARRAY['warehouse_old', 'aps_old', 'old_worker'];
BEGIN
    labels := COALESCE(NEW.routing_labels, '[]'::jsonb);
    IF jsonb_typeof(NEW.metadata -> 'warehouse_labels') = 'array' THEN
        labels := labels || (NEW.metadata -> 'warehouse_labels');
    END IF;
    IF jsonb_typeof(NEW.metadata -> 'labels') = 'array' THEN
        labels := labels || (NEW.metadata -> 'labels');
    END IF;

    explicit := lower(COALESCE(
        NEW.metadata ->> 'warehouse_protocol',
        NEW.metadata ->> 'protocol',
        NEW.metadata ->> 'api_version',
        NEW.protocol
    ));
    explicit := CASE
        WHEN explicit IN ('1', 'v1', 'legacy', 'cents', 'minor') THEN 'v1'
        WHEN explicit IN ('2', 'v2', 'new', 'euros', 'major') THEN 'v2'
        ELSE explicit
    END;

    IF explicit IN ('v1', 'v2') THEN
        NEW.protocol := explicit;
        NEW.routing_status := 'ready';
        NEW.classification_reason := 'explicit protocol';
    ELSIF NEW.routing_status = 'manual_review'
       OR labels ? 'manual_review'
       OR labels ? 'quarantined' THEN
        NEW.protocol := COALESCE(NEW.protocol, 'unknown');
        NEW.routing_status := 'manual_review';
        NEW.classification_reason := COALESCE(
            NEW.classification_reason, 'marked for manual review'
        );
    ELSIF labels ? 'legacy_ingest'
       OR lower(NEW.task_id) LIKE '%legacy_ingest%'
       OR lower(COALESCE(NEW.metadata ->> 'source', ''))
          IN ('legacy_ingest', 'historical_legacy') THEN
        NEW.protocol := 'v1';
        NEW.routing_status := 'ready';
        NEW.classification_reason := 'legacy_ingest is v1-only';
    ELSIF current_user = ANY(old_roles)
       OR lower(COALESCE(NEW.metadata ->> 'source', ''))
          IN ('old_add_job', 'old_schedule', 'old_process') THEN
        NEW.protocol := 'v1';
        NEW.routing_status := 'ready';
        NEW.classification_reason := 'unchanged v1 process entry';
    ELSIF labels ? 'threadpool' OR NEW.executor = 'threadpool' THEN
        NEW.protocol := 'unknown';
        NEW.routing_status := 'manual_review';
        NEW.classification_reason :=
            'threadpool was used by both v1 and v2 and business params are ambiguous';
    ELSE
        NEW.protocol := COALESCE(
            current_setting('warehouse.default_protocol', true), 'v1'
        );
        NEW.routing_status := 'ready';
        NEW.classification_reason := 'submitter default protocol';
    END IF;

    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_warehouse_classify_job ON jobs;
CREATE TRIGGER trg_warehouse_classify_job
    BEFORE INSERT OR UPDATE OF protocol, metadata, routing_labels, task_id, executor
    ON jobs
    FOR EACH ROW
    EXECUTE FUNCTION warehouse_classify_job();

-- Backfill known historical rows. The trigger performs the same classification
-- for UPDATE, but these statements make the migration intent explicit.
UPDATE jobs
SET protocol = lower(COALESCE(
        metadata ->> 'warehouse_protocol', metadata ->> 'protocol', protocol
    )),
    classification_reason = 'explicit protocol'
WHERE protocol IS NULL
  AND lower(COALESCE(
        metadata ->> 'warehouse_protocol', metadata ->> 'protocol', ''
      )) IN ('v1', 'v2', '1', '2');

UPDATE jobs
SET protocol = 'v1',
    routing_status = 'ready',
    classification_reason = 'legacy_ingest is v1-only'
WHERE protocol IS NULL
  AND (
      task_id ILIKE '%legacy_ingest%'
      OR metadata ? 'legacy_ingest'
      OR COALESCE(metadata -> 'warehouse_labels', '[]'::jsonb) ? 'legacy_ingest'
  );

UPDATE jobs
SET protocol = 'unknown',
    routing_status = 'manual_review',
    classification_reason =
        'threadpool was used by both v1 and v2 and business params are ambiguous'
WHERE protocol IS NULL
  AND executor = 'threadpool';

INSERT INTO warehouse_manual_review (
    job_id, status, reason, original_args, original_kwargs, labels
)
SELECT
    jobs.id,
    'awaiting_manual_verification',
    jobs.classification_reason,
    jobs.args,
    jobs.kwargs,
    jobs.routing_labels
FROM jobs
WHERE jobs.routing_status = 'manual_review'
ON CONFLICT (job_id) DO NOTHING;

-- Old role: stock APScheduler queries gain an implicit v1-only filter.
ALTER TABLE jobs ENABLE ROW LEVEL SECURITY;
ALTER TABLE jobs FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS warehouse_old_job_isolation ON jobs;
CREATE POLICY warehouse_old_job_isolation ON jobs
    FOR ALL
    TO warehouse_old
    USING (
        task_id NOT IN ('warehouse_ingest', 'legacy_ingest')
        OR (routing_status = 'ready' AND protocol = 'v1')
    )
    WITH CHECK (
        task_id NOT IN ('warehouse_ingest', 'legacy_ingest')
        OR (routing_status = 'ready' AND protocol = 'v1')
    );

-- New role can run both confirmed protocols, but never unresolved history.
DROP POLICY IF EXISTS warehouse_new_job_isolation ON jobs;
CREATE POLICY warehouse_new_job_isolation ON jobs
    FOR ALL
    TO warehouse_new
    USING (
        task_id NOT IN ('warehouse_ingest', 'legacy_ingest')
        OR (routing_status = 'ready' AND protocol IN ('v1', 'v2'))
    )
    WITH CHECK (
        task_id NOT IN ('warehouse_ingest', 'legacy_ingest')
        OR (routing_status = 'ready' AND protocol IN ('v1', 'v2'))
    );

GRANT USAGE ON SCHEMA public TO warehouse_old, warehouse_new;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public
    TO warehouse_old, warehouse_new;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public
    TO warehouse_old, warehouse_new;

-- Receipts and review status must remain queryable by both processes.
GRANT SELECT, INSERT, UPDATE ON warehouse_receipts, warehouse_manual_review
    TO warehouse_old, warehouse_new;

COMMIT;
