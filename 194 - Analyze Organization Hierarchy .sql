# DAY 194 
# 3482. Analyze Organization Hierarchy
WITH RECURSIVE Org AS (
    SELECT 
        employee_id,
        employee_name,
        manager_id,
        1 AS level
    FROM Employees
    WHERE manager_id IS NULL

    UNION ALL

    SELECT 
        e.employee_id,
        e.employee_name,
        e.manager_id,
        o.level + 1
    FROM Employees e
    JOIN Org o
    ON e.manager_id = o.employee_id
)
SELECT *
FROM Org;