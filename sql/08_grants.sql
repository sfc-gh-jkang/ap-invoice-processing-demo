-- =============================================================================
-- 08_grants.sql — Grant access on all objects to a specified role
-- =============================================================================
-- This script is idempotent and can be re-run whenever new objects are created.
--
-- Usage (demo account):
--   snow sql -c default -f sql/08_grants.sql
--
-- Usage (Snowhouse / shared environments):
--   snow sql -c snowhouse -f sql/08_grants.sql
--
-- NOTE: GRANT ... ON FUTURE TABLES/VIEWS requires MANAGE GRANTS privilege.
-- If your role lacks MANAGE GRANTS, re-run this script after creating new
-- objects, or ask an administrator to set up future grants:
--
--   GRANT SELECT ON FUTURE TABLES IN SCHEMA <db>.<schema> TO ROLE <role>;
--   GRANT SELECT ON FUTURE VIEWS  IN SCHEMA <db>.<schema> TO ROLE <role>;
-- =============================================================================

-- Adjust these variables for your environment:
SET target_role = 'SALES_ENGINEER';

-- Schema-level access
GRANT USAGE ON SCHEMA IDENTIFIER(CURRENT_DATABASE() || '.' || CURRENT_SCHEMA())
    TO ROLE IDENTIFIER($target_role);

-- Tables: SELECT + DML
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA
    IDENTIFIER(CURRENT_DATABASE() || '.' || CURRENT_SCHEMA())
    TO ROLE IDENTIFIER($target_role);

-- Views: SELECT
GRANT SELECT ON ALL VIEWS IN SCHEMA
    IDENTIFIER(CURRENT_DATABASE() || '.' || CURRENT_SCHEMA())
    TO ROLE IDENTIFIER($target_role);

-- Stages: READ + WRITE
GRANT READ, WRITE ON ALL STAGES IN SCHEMA
    IDENTIFIER(CURRENT_DATABASE() || '.' || CURRENT_SCHEMA())
    TO ROLE IDENTIFIER($target_role);

-- Stored procedures: USAGE
GRANT USAGE ON ALL PROCEDURES IN SCHEMA
    IDENTIFIER(CURRENT_DATABASE() || '.' || CURRENT_SCHEMA())
    TO ROLE IDENTIFIER($target_role);

-- Functions: USAGE
GRANT USAGE ON ALL FUNCTIONS IN SCHEMA
    IDENTIFIER(CURRENT_DATABASE() || '.' || CURRENT_SCHEMA())
    TO ROLE IDENTIFIER($target_role);

-- Streamlit apps: USAGE
GRANT USAGE ON ALL STREAMLITS IN SCHEMA
    IDENTIFIER(CURRENT_DATABASE() || '.' || CURRENT_SCHEMA())
    TO ROLE IDENTIFIER($target_role);

-- Attempt future grants (will fail silently if MANAGE GRANTS is missing)
-- Uncomment these if your role has MANAGE GRANTS:
-- GRANT SELECT ON FUTURE TABLES IN SCHEMA IDENTIFIER(CURRENT_DATABASE() || '.' || CURRENT_SCHEMA()) TO ROLE IDENTIFIER($target_role);
-- GRANT SELECT ON FUTURE VIEWS  IN SCHEMA IDENTIFIER(CURRENT_DATABASE() || '.' || CURRENT_SCHEMA()) TO ROLE IDENTIFIER($target_role);
