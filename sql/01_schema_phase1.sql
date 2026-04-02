-- ============================================================
-- B2B CRM -- Phase 1 Schema
-- Run this ONCE in psql or pgAdmin before running etl.py
-- ============================================================

-- CREATE DATABASE crm_db;
-- \c crm_db

-- TABLE 1: companies
CREATE TABLE IF NOT EXISTS companies (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(255) NOT NULL,
    name_normalized VARCHAR(255) GENERATED ALWAYS AS (LOWER(TRIM(name))) STORED,
    industry        VARCHAR(100),
    raw_address     TEXT,
    website         VARCHAR(255),
    size            VARCHAR(50),
    status          VARCHAR(50) NOT NULL DEFAULT 'prospect'
                    CHECK (status IN ('prospect', 'active', 'churned')),
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_companies_name_normalized
    ON companies(name_normalized);

CREATE INDEX IF NOT EXISTS idx_companies_name_normalized
    ON companies(name_normalized);


-- TABLE 2: contacts
-- email lives in contact_emails table
CREATE TABLE IF NOT EXISTS contacts (
    id              SERIAL PRIMARY KEY,
    source          VARCHAR(30) NOT NULL
                    CHECK (source IN ('rocketreach', 'msme', 'pharma', 'manual')),
    source_id       VARCHAR(50),
    company_id      INT REFERENCES companies(id) ON DELETE SET NULL,
    first_name      VARCHAR(100),
    last_name       VARCHAR(100),
    designation     VARCHAR(150),
    is_primary      BOOLEAN NOT NULL DEFAULT FALSE,
    is_duplicate_of INT REFERENCES contacts(id) ON DELETE SET NULL,
    status          VARCHAR(50) NOT NULL DEFAULT 'active'
                    CHECK (status IN ('active', 'inactive', 'invalid')),
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_contacts_source_source_id
    ON contacts(source, source_id)
    WHERE source_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_contacts_source
    ON contacts(source);

CREATE INDEX IF NOT EXISTS idx_contacts_company_id
    ON contacts(company_id);


-- TABLE 3: contact_emails
-- One row per email. rank 1 = primary.
CREATE TABLE IF NOT EXISTS contact_emails (
    id           SERIAL PRIMARY KEY,
    contact_id   INT NOT NULL REFERENCES contacts(id) ON DELETE CASCADE,
    email        VARCHAR(255) NOT NULL,
    rank         SMALLINT NOT NULL DEFAULT 1
                 CHECK (rank >= 1),
    created_at   TIMESTAMP NOT NULL DEFAULT NOW(),

    UNIQUE (contact_id, email),
    UNIQUE (contact_id, rank)
);

CREATE INDEX IF NOT EXISTS idx_contact_emails_email
    ON contact_emails(email);

CREATE INDEX IF NOT EXISTS idx_contact_emails_contact_id
    ON contact_emails(contact_id);


-- TABLE 4: contact_phones
-- One row per phone. rank 1 = primary. Max 3 enforced by ETL.
CREATE TABLE IF NOT EXISTS contact_phones (
    id              SERIAL PRIMARY KEY,
    contact_id      INT NOT NULL REFERENCES contacts(id) ON DELETE CASCADE,
    phone_number    VARCHAR(20) NOT NULL,
    rank            SMALLINT NOT NULL DEFAULT 1
                    CHECK (rank BETWEEN 1 AND 3),
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),

    UNIQUE (contact_id, phone_number),
    UNIQUE (contact_id, rank)
);

CREATE INDEX IF NOT EXISTS idx_contact_phones_number
    ON contact_phones(phone_number);

CREATE INDEX IF NOT EXISTS idx_contact_phones_contact_id
    ON contact_phones(contact_id);


-- FLAT VIEW: contacts_flat
-- Use in pgAdmin to browse data without writing JOINs.
CREATE OR REPLACE VIEW contacts_flat AS
SELECT
    c.id,
    c.source,
    c.source_id,
    c.first_name,
    c.last_name,
    c.designation,
    c.status,
    co.name                                                   AS company_name,
    co.raw_address,
    MAX(CASE WHEN e.rank = 1 THEN e.email END)                AS email1,
    MAX(CASE WHEN e.rank = 2 THEN e.email END)                AS email2,
    MAX(CASE WHEN e.rank = 3 THEN e.email END)                AS email3,
    MAX(CASE WHEN p.rank = 1 THEN p.phone_number END)         AS phone1,
    MAX(CASE WHEN p.rank = 2 THEN p.phone_number END)         AS phone2,
    MAX(CASE WHEN p.rank = 3 THEN p.phone_number END)         AS phone3
FROM contacts c
LEFT JOIN companies      co ON co.id        = c.company_id
LEFT JOIN contact_emails e  ON e.contact_id = c.id
LEFT JOIN contact_phones p  ON p.contact_id = c.id
GROUP BY c.id, c.source, c.source_id, c.first_name, c.last_name,
         c.designation, c.status, co.name, co.raw_address;
