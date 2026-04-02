-- Step 1: Delete call_actions linked to today's allocations
DELETE FROM call_actions
WHERE allocation_id IN (
    SELECT id FROM contact_allocations
    WHERE allocated_date = '2026-03-13'
);

-- Step 2: Delete the allocations themselves
DELETE FROM contact_allocations
WHERE allocated_date = '2026-03-13';

-- Step 3: Reset contact_flags back to 'fresh' for those contacts
-- (only the ones that were fresh before today — i.e. had no prior history)
UPDATE contacts
SET contact_flag = 'fresh',
    flag_updated_at = NULL
WHERE id IN (
    -- contacts whose ONLY call history was from today
    SELECT c.id
    FROM contacts c
    WHERE NOT EXISTS (
        SELECT 1 FROM call_actions ca
        WHERE ca.contact_id = c.id
    )
    AND contact_flag != 'fresh'
);

-- Verify everything is clean
SELECT COUNT(*) FROM contact_allocations WHERE allocated_date = '2026-03-13';
SELECT COUNT(*) FROM call_actions WHERE called_at::date = '2026-03-13';
