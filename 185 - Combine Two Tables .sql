# DAY 185
# 175. Combine Two Tables
# 175. Combine Two Tables

SELECT firstName, lastName, city, state
FROM Person p
LEFT JOIN Address a
ON p.personId = a.personId;