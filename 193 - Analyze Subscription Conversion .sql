# DAY 192 
# 3497. Analyze Subscription Conversion 
SELECT 
    user_id,
    ROUND(AVG(activity_type = 'paid'), 2) AS conversion_rate
FROM UserActivity
WHERE activity_type IN ('free_trial', 'paid')
GROUP BY user_id;