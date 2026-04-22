-- UNIQUE_ISSUES TABLE
UPDATE `{{ curated.UNIQUE_ISSUES }}`
SET
    mean_size = CASE t_shirt_size
        WHEN '2xExtra Small' THEN 0.5
        WHEN 'Extra Small' THEN 1.5
        WHEN 'Small' THEN 6.0
        WHEN 'Medium' THEN 12.0
        WHEN 'Large' THEN 19.5
        WHEN 'Extra Large' THEN 39.0
        WHEN '2xExtra Large' THEN 49.5
        WHEN '3xExtra Large' THEN 90.0
        WHEN '4xExtra Large' THEN 150.0
        ELSE 0.0
    END
WHERE t_shirt_size IS NOT NULL;

-- T_SHIRT_SIZE_CHANGELOG TABLE
UPDATE `{{ curated.T_SHIRT_SIZE_CHANGELOG }}`
SET
    from_mean_size = CASE from_t_shirt_size
        WHEN '2xExtra Small' THEN 0.5
        WHEN 'Extra Small' THEN 1.5
        WHEN 'Small' THEN 6.0
        WHEN 'Medium' THEN 12.0
        WHEN 'Large' THEN 19.5
        WHEN 'Extra Large' THEN 39.0
        WHEN '2xExtra Large' THEN 49.5
        WHEN '3xExtra Large' THEN 90.0
        WHEN '4xExtra Large' THEN 150.0
        ELSE 0.0
    END,
    to_mean_size = CASE to_t_shirt_size
        WHEN '2xExtra Small' THEN 0.5
        WHEN 'Extra Small' THEN 1.5
        WHEN 'Small' THEN 6.0
        WHEN 'Medium' THEN 12.0
        WHEN 'Large' THEN 19.5
        WHEN 'Extra Large' THEN 39.0
        WHEN '2xExtra Large' THEN 49.5
        WHEN '3xExtra Large' THEN 90.0
        WHEN '4xExtra Large' THEN 150.0
        ELSE 0.0
    END
-- WHERE clause required for execution of UPDATE Statement
WHERE issue_key IS NOT NULL;
