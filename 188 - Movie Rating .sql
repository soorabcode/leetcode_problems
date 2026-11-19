# DAY 188
# 1341. Movie Rating
(SELECT name AS results
FROM MovieRating mr
JOIN Users u ON mr.user_id = u.user_id
GROUP BY mr.user_id
ORDER BY COUNT(*) DESC, name
LIMIT 1)

UNION ALL

(SELECT title AS results
FROM MovieRating mr
JOIN Movies m ON mr.movie_id = m.movie_id
WHERE created_at >= '2020-02-01'
  AND created_at < '2020-03-01'
GROUP BY mr.movie_id
ORDER BY AVG(rating) DESC, title
LIMIT 1);