-- Migration: Add bd_category to contacts
-- This column stores the original Category from BD sheets/imports.

ALTER TABLE contacts
    ADD COLUMN IF NOT EXISTS bd_category VARCHAR(100);

CREATE INDEX IF NOT EXISTS idx_contacts_bd_category
    ON contacts(bd_category);
