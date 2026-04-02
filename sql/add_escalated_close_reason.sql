-- Add literal 'escalated' support to mql_allocations.close_reason.
-- Safe to run multiple times.

DO $$
DECLARE
    r record;
BEGIN
    FOR r IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'mql_allocations'::regclass
          AND contype = 'c'
          AND pg_get_constraintdef(oid) ILIKE '%close_reason%'
    LOOP
        EXECUTE format('ALTER TABLE mql_allocations DROP CONSTRAINT IF EXISTS %I', r.conname);
    END LOOP;

    ALTER TABLE mql_allocations
        ADD CONSTRAINT mql_allocations_close_reason_check
        CHECK (close_reason IN (
            'qualified', 'rejected', 'stalled',
            'reallocated', 'escalated', 'bd_history'
        ));
END $$;
