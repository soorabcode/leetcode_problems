# Day 156 
# Nth Highest Salary 
# Write a solution to find the nth highest distinct salary from the Employee table. If there are less than n distinct salaries, return null.
# Leetcode 177

CREATE FUNCTION getNthHighestSalary(N INT) RETURNS INT
BEGIN
  SET N = N - 1;
  RETURN (
      SELECT DISTINCT salary
      FROM Employee
      ORDER BY salary DESC
      LIMIT N, 1
  );
END